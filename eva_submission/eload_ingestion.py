import os
import subprocess
from pathlib import Path

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_pg_metadata_uri_for_eva_profile, get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
import psycopg2
import pymongo

from eva_submission import ROOT_DIR
from eva_submission.eload_submission import Eload
from eva_submission.ingestion_templates import accession_props_template, variant_load_props_template

project_dirs = {
    'logs': '00_logs',
    'valid': '30_eva_valid',
    'transformed': '40_transformed',
    'stats': '50_stats',
    'annotation': '51_annotation',
    'accessions': '52_accessions',
    'public': '60_eva_public',
    'external': '70_external_submissions',
    'deprecated': '80_deprecated'
}


class EloadIngestion(Eload):
    config_section = 'ingestion'  # top-level config key
    all_tasks = ['metadata_load', 'accession', 'variant_load']

    def __init__(self, eload_number):
        super().__init__(eload_number)
        self.eload_cfg.set(self.config_section, 'ingestion_date', value=self.now)
        self.settings_xml_file = cfg['maven_settings_file']
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')

    def ingest(self, aggregation, instance_id, db_name=None, tasks=None):
        # TODO assembly/taxonomy insertion script should be incorporated here
        # TODO set ENA data release date
        self.check_variant_db(db_name)

        if not tasks:
            tasks = self.all_tasks

        if 'metadata_load' in tasks:
            self.load_from_ena()
        # TODO are accession and variant load independent tasks?
        if 'accession' in tasks or 'variant_load' in tasks:
            self.accession_and_load(aggregation, instance_id)

    def get_db_name(self):
        """
        Constructs the expected database name in mongo, based on assembly info retrieved from EVAPRO.
        """
        assm_accession = self.eload_cfg.query('submission', 'assembly_accession')
        taxon_id = self.eload_cfg.query('submission', 'taxonomy_id')

        # query EVAPRO for db name based on taxonomy id and accession
        pg_uri = get_pg_metadata_uri_for_eva_profile("development", self.settings_xml_file)
        with psycopg2.connect(pg_uri) as conn:
            query = (
                "SELECT b.taxonomy_code, a.assembly_code "
                "FROM evapro.assembly a "
                "JOIN evapro.taxonomy b on b.taxonomy_id = a.taxonomy_id "
                f"WHERE a.taxonomy_id = '{taxon_id}' "
                f"AND a.assembly_accession = '{assm_accession}';"
            )
            rows = get_all_results_for_query(conn, query)
        # we should get exactly one result, if not, fail loudly.
        if len(rows) == 0:
            self.error(f'Database for taxonomy id {taxon_id} and assembly {assm_accession} not found in EVAPRO.')
            self.error(f'Please insert the appropriate taxonomy and assembly or pass in the database name explicitly.')
            # TODO propose a database name, based on a TBD convention
            raise ValueError(f'No database for {taxon_id} and {assm_accession} found')
        elif len(rows) > 1:
            self.error(f'Found more than one possible database, please pass in the database name explicitly.')
            options = ', '.join((f'{r[0]}_{r[1]}' for r in rows))
            self.error(f'Options found: {options}')
            raise ValueError(f'More than one possible database for {taxon_id} and {assm_accession} found')
        return f'eva_{rows[0][0]}_{rows[0][1]}'

    def check_variant_db(self, db_name=None):
        """
        Checks mongo for the right variant database.
        If db_name is provided it will check for that, otherwise it will construct the expected database name.
        """
        if not db_name:
            db_name = self.get_db_name()
        self.eload_cfg.set(self.config_section, 'database', 'db_name', value=db_name)

        mongo_uri = get_mongo_uri_for_eva_profile("production", self.settings_xml_file)
        with pymongo.MongoClient(mongo_uri) as db:
            names = db.list_database_names()
            if db_name in names:
                self.info(f'Found database named {db_name}.')
                self.info('If this is incorrect, please cancel and pass in the database name explicitly.')
                self.eload_cfg.set(self.config_section, 'database', 'exists', value=True)
            else:
                self.error(f'Database named {db_name} does not exist in variant warehouse, aborting.')
                self.error('Please create the database or pass in the appropriate database name explicitly.')
                self.eload_cfg.set(self.config_section, 'database', 'exists', value=False)
                raise ValueError(f'No database named {db_name} found.')

    def load_from_ena(self):
        """
        Loads project metadata from ENA into EVADEV.
        """
        if not self.project_accession:
            self.error('No project accession in submission config, check that brokering to ENA is done. ')
            raise ValueError('No project accession in submission config.')
        try:
            command_utils.run_command_with_output(
                'Load metadata from ENA to EVADEV',
                ' '.join((
                    'perl', cfg['executable']['load_from_ena'],
                    '-p', self.project_accession,
                    # Current submission process never changes -c or -v
                    '-c', 'submitted',
                    '-v', '1',
                    # -l is only checked for when -c=eva_value_added, so in reality never used
                    '-l', self._get_dir('scratch'),
                    '-e', str(self.eload_num)
                ))
            )
            self.eload_cfg.set(self.config_section, 'ena_load', value='success')
        except subprocess.CalledProcessError as e:
            self.error('ENA metadata load failed: aborting ingestion.')
            self.eload_cfg.set(self.config_section, 'ena_load', value='failure')
            raise e

    def accession_and_load(self, aggregation, instance_id):
        self.eload_cfg.set(self.config_section, 'aggregation', aggregation)
        self.eload_cfg.set(self.config_section, 'accession', 'instance_id', instance_id)

        project_dir = self.create_project_dir()
        self.eload_cfg.set(self.config_section, 'project_dir', project_dir)

        prop_files = self.create_accession_properties()
        self.eload_cfg.set(self.config_section, 'accession', 'properties', prop_files)
        prop_files = self.create_variant_load_properties()
        self.eload_cfg.set(self.config_section, 'variant_load', 'properties', prop_files)

        self.run_ingestion_workflow()

    def create_project_dir(self):
        project_dir = Path(cfg['projects_dir'], self.project_accession)
        os.makedirs(project_dir, exist_ok=True)
        for _, v in project_dirs.items():
            os.makedirs(project_dir.joinpath(project_dirs[v]), exist_ok=True)
        # TODO do we need the links to submitted/scratch?
        # TODO need to copy valid vcfs + index to 'valid'!
        return project_dir

    def create_accession_properties(self):
        project_dir = self.eload_cfg(self.config_section, 'project_dir')
        prop_files = []
        for vcf_path in project_dir.joinpath(project_dirs['valid']).glob('*.vcf.gz'):
            filename = vcf_path.stem
            output_vcf = project_dir.joinpath(project_dirs['public'], f'{filename}.accessioned.vcf')

            properties_filename = project_dir.joinpath(project_dirs['accessions'], f'{filename}.properties')
            with open(properties_filename, 'w+') as f:
                f.write(accession_props_template(
                    assembly_accession=self.eload_cfg.query('submission', 'assembly_accession'),
                    taxonomy_id=self.eload_cfg.query('submission', 'taxonomy_id'),
                    project_accession=self.project_accession,
                    aggregation=self.eload_cfg.query(self.config_section, 'aggregation'),
                    fasta=self.eload_cfg.query('submission', 'assembly_fasta'),
                    report=self.eload_cfg('submission', 'assembly_report'),
                    instance_id=self.eload.cfg.query(self.config_section, 'accession', 'instance_id'),
                    vcf_path=vcf_path,
                    output_vcf=output_vcf,
                    # TODO db creds from settings xml file...
                ))
            prop_files.append(properties_filename)
        return prop_files

    def create_variant_load_properties(self):
        # like accession props we want one per vcf file
        project_dir = self.eload_cfg(self.config_section, 'project_dir')
        prop_files = []
        for vcf_path in project_dir.joinpath(project_dirs['valid']).glob('*.vcf.gz'):
            filename = vcf_path.stem
            properties_filename = project_dir.joinpath(f'load_{filename}.properties')
            with open(properties_filename, 'w+') as f:
                f.write(variant_load_props_template(
                    project_accession=self.project_accession,
                    analysis_accession=self.eload_cfg.query('brokering', 'ena', 'ANALYSIS'),
                    vcf_path=vcf_path,
                    aggregation=self.eload_cfg.query(self.config_section, 'aggregation'),
                    study_name='',  # TODO - from metadata (also double check other params)
                    fasta=self.eload_cfg.query('submission', 'assembly_fasta'),
                    db_name=self.eload_cfg.query(self.config_section, 'database', 'db_name'),
                    species=self.get_vep_species()
                ))
            prop_files.append(properties_filename)
        return prop_files

    def get_vep_species(self):
        # TODO is this right...
        words = self.eload_cfg.query('submission', 'scientific name').lower().split()
        return '_'.join(words)

    def run_ingestion_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory()
        ingestion_config = {
            'accession_props': self.eload_cfg.query(self.config_section, 'accession', 'properties'),
            'variant_load_props': self.eload_cfg.query(self.config_section, 'variant_load', 'properties'),
            'eva_pipeline_props': cfg['eva_pipeline_props'],
            'output_dir': output_dir,
            'executable': cfg['executable'],
            'jar': cfg['jar'],
        }
        ingestion_config_file = os.path.join(self.eload_dir, 'ingestion_config_file.yaml')
        with open(ingestion_config_file, 'w') as open_file:
            yaml.safe_dump(ingestion_config, open_file)
        ingestion_script = os.path.join(ROOT_DIR, 'nextflow', 'ingestion.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow Ingestion process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], ingestion_script,
                    '-params-file', ingestion_config_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError:
            self.error('Nextflow pipeline failed: results might not be complete')
        return output_dir
