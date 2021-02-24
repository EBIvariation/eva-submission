import os
import shutil
import subprocess
from pathlib import Path

import yaml
from cached_property import cached_property
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile, get_properties_from_xml_file
from ebi_eva_common_pyutils.metadata_utils import get_variant_warehouse_db_name_from_assembly_and_taxonomy
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
import psycopg2
import pymongo

from eva_submission import ROOT_DIR
from eva_submission.eload_submission import Eload
from eva_submission.ingestion_templates import accession_props_template, variant_load_props_template

project_dirs = {
    'logs': '00_logs',
    'valid': '30_eva_valid',
    'merged': '31_merged',
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
        self.settings_xml_file = cfg['maven_settings_file']
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        self.project_dir = self.setup_project_dir()
        self.mongo_uri = get_mongo_uri_for_eva_profile('production', self.settings_xml_file)

    def ingest(
            self,
            aggregation=None,
            instance_id=None,
            vep_version=None,
            vep_cache_version=None,
            db_name=None,
            tasks=None
    ):
        # TODO assembly/taxonomy insertion script should be incorporated here
        # TODO set ENA data release date (EVA-2322)
        self.eload_cfg.set(self.config_section, 'ingestion_date', value=self.now)
        self.check_variant_db(db_name)

        if not tasks:
            tasks = self.all_tasks

        if 'metadata_load' in tasks:
            self.load_from_ena()
        do_accession = 'accession' in tasks
        do_variant_load = 'variant_load' in tasks

        if do_accession or do_variant_load:
            aggregation = aggregation.lower()
            self.eload_cfg.set(self.config_section, 'aggregation', value=aggregation)

        if do_accession:
            self.eload_cfg.set(self.config_section, 'accession', 'instance_id', value=instance_id)
            accession_prop_files = self.create_accession_properties()
            self.eload_cfg.set(self.config_section, 'accession', 'properties', value=accession_prop_files)
            self.run_accession_workflow()

        if do_variant_load:
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'version', value=vep_version)
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'cache_version', value=vep_cache_version)
            if aggregation == 'none':
                merged_files = self.merge_vcfs()
                self.vcfs_to_load = merged_files
            else:
                self.vcfs_to_load = self.valid_vcf_filenames
            self.eload_cfg.set(self.config_section, 'variant_load', 'vcfs_to_load', value=[str(x) for x in self.vcfs_to_load])

            load_prop_files = self.create_variant_load_properties()
            self.eload_cfg.set(self.config_section, 'variant_load', 'properties', value=load_prop_files)
            self.run_variant_load_workflow()

    def get_db_name(self):
        """
        Constructs the expected database name in mongo, based on assembly info retrieved from EVAPRO.
        """
        assm_accession = self.eload_cfg.query('submission', 'assembly_accession')
        taxon_id = self.eload_cfg.query('submission', 'taxonomy_id')
        # query EVAPRO for db name based on taxonomy id and accession
        with self.get_pg_conn() as conn:
            db_name = get_variant_warehouse_db_name_from_assembly_and_taxonomy(conn, assm_accession, taxon_id)
        if not db_name:
            self.error(f'Database for taxonomy id {taxon_id} and assembly {assm_accession} not found in EVAPRO.')
            self.error(f'Please insert the appropriate taxonomy and assembly or pass in the database name explicitly.')
            # TODO propose a database name, based on a TBD convention
            # TODO download the VEP cache for new species
            raise ValueError(f'No database for {taxon_id} and {assm_accession} found')
        return db_name

    def check_variant_db(self, db_name=None):
        """
        Checks mongo for the right variant database.
        If db_name is provided it will check for that, otherwise it will construct the expected database name.
        """
        if not db_name:
            db_name = self.get_db_name()
        self.eload_cfg.set(self.config_section, 'database', 'db_name', value=db_name)

        with pymongo.MongoClient(self.mongo_uri) as db:
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

    def setup_project_dir(self):
        """
        Sets up project directory and copies VCF files from the eload directory.
        """
        project_dir = Path(cfg['projects_dir'], self.project_accession)
        os.makedirs(project_dir, exist_ok=True)
        for _, v in project_dirs.items():
            os.makedirs(project_dir.joinpath(v), exist_ok=True)
        # copy valid vcfs + index to 'valid' folder
        valid_dir = project_dir.joinpath(project_dirs['valid'])
        vcf_dict = self.eload_cfg.query('brokering', 'vcf_files')
        if vcf_dict is None:
            self.error('No brokered VCF files found, aborting ingestion.')
            raise ValueError('No brokered VCF files found.')
        for key, val in vcf_dict.items():
            vcf_path = Path(key)
            shutil.copyfile(vcf_path, valid_dir.joinpath(vcf_path.name))
            index_path = Path(val['index'])
            shutil.copyfile(index_path, valid_dir.joinpath(index_path.name))
        self.eload_cfg.set(self.config_section, 'project_dir', value=str(project_dir))
        return project_dir

    def merge_vcfs(self):
        """
        Attempts to merge valid VCFs by sample.
        If duplicate sample names are detected, this will assume files are split horizontally
        and so no merge is necessary.
        Returns the list of filenames that should be used for the load pipeline, i.e. either
        a singleton list with the merged filename, or the list of valid VCFs.
        """
        merged_dir = self.project_dir.joinpath(project_dirs['merged'])
        output_file = str(merged_dir.joinpath(f'{self.project_accession}_merged.vcf'))
        list_file = str(merged_dir.joinpath('all_files.list'))
        with open(list_file, 'w+') as f:
            f.write('\n'.join(str(fn) for fn in self.valid_vcf_filenames))
        try:
            command_utils.run_command_with_output(
                'Merge VCFs',
                ' '.join((
                    cfg['executable']['bcftools'], 'merge'
                    '--merge', 'all',
                    '--file-list', list_file,
                    '--threads', '3',
                    '-o', output_file
                ))
            )
            command_utils.run_command_with_output(
                'Bgzip merged vcf file',
                ' '.join((cfg['executable']['bgzip'], output_file))
            )
            return [Path(f'{output_file}.gz')]
        except subprocess.CalledProcessError as e:
            if e.output is not None and 'Duplicate sample names' in e.output:  # TODO brittle
                self.warning('Duplicate sample names found while merging, will continue with unmerged VCFs.')
                return self.valid_vcf_filenames
            self.error('Merging VCFs failed: aborting ingestion.')
            self.eload_cfg.set(self.config_section, 'variant_load', 'vcfs_to_load', value='merge failed')
            raise e

    def create_accession_properties(self):
        """
        Creates properties files for the accessioning pipeline, one for each VCF file.
        """
        prop_files = []
        # TODO change accession pipeline to get db creds from a common properties file, like variant load?
        # then we won't need this bit
        mongo_host, mongo_user, mongo_pass = self.get_mongo_creds()
        pg_url, pg_user, pg_pass = self.get_pg_creds()
        for vcf_path in self.valid_vcf_filenames:
            filename = vcf_path.stem
            output_vcf = self.project_dir.joinpath(project_dirs['public'], f'{filename}.accessioned.vcf')

            properties_filename = self.project_dir.joinpath(project_dirs['accessions'], f'{filename}.properties')
            with open(properties_filename, 'w+') as f:
                f.write(accession_props_template(
                    assembly_accession=self.eload_cfg.query('submission', 'assembly_accession'),
                    taxonomy_id=self.eload_cfg.query('submission', 'taxonomy_id'),
                    project_accession=self.project_accession,
                    aggregation=self.eload_cfg.query(self.config_section, 'aggregation'),
                    fasta=self.eload_cfg.query('submission', 'assembly_fasta'),
                    report=self.eload_cfg.query('submission', 'assembly_report'),
                    instance_id=self.eload_cfg.query(self.config_section, 'accession', 'instance_id'),
                    vcf_path=vcf_path,
                    output_vcf=output_vcf,
                    mongo_host=mongo_host,
                    mongo_user=mongo_user,
                    mongo_pass=mongo_pass,
                    postgres_url=pg_url,
                    postgres_user=pg_user,
                    postgres_pass=pg_pass
                ))
            prop_files.append(str(properties_filename))
        return prop_files

    def create_variant_load_properties(self):
        """
        Creates properties files for the variant load pipeline, one for each VCF file.
        """
        prop_files = []
        for vcf_path in self.vcfs_to_load:
            filename = vcf_path.stem
            properties_filename = self.project_dir.joinpath(f'load_{filename}.properties')
            with open(properties_filename, 'w+') as f:
                f.write(variant_load_props_template(
                    project_accession=self.project_accession,
                    analysis_accession=self.eload_cfg.query('brokering', 'ena', 'ANALYSIS'),
                    vcf_path=vcf_path,
                    aggregation=self.eload_cfg.query(self.config_section, 'aggregation'),
                    study_name=self.get_study_name(),
                    fasta=self.eload_cfg.query('submission', 'assembly_fasta'),
                    output_dir=self.project_dir.joinpath(project_dirs['transformed']),
                    annotation_dir=self.project_dir.joinpath(project_dirs['annotation']),
                    stats_dir=self.project_dir.joinpath(project_dirs['stats']),
                    db_name=self.eload_cfg.query(self.config_section, 'database', 'db_name'),
                    vep_species=self.get_vep_species(),
                    vep_version=self.eload_cfg.query(self.config_section, 'variant_load', 'vep', 'version'),
                    vep_cache_version=self.eload_cfg.query(self.config_section, 'variant_load', 'vep', 'cache_version')
                ))
            prop_files.append(str(properties_filename))
        return prop_files

    def get_mongo_creds(self):
        properties = get_properties_from_xml_file('production', self.settings_xml_file)
        mongo_host = str(str(properties['eva.mongo.host']).split(',')[0]).split(':')[0]
        mongo_user = properties['eva.mongo.user']
        mongo_pass = properties['eva.mongo.passwd']
        return mongo_host, mongo_user, mongo_pass

    def get_pg_creds(self):
        properties = get_properties_from_xml_file('development', self.settings_xml_file)
        pg_url = properties['eva.evapro.jdbc.url']
        pg_user = properties['eva.evapro.user']
        pg_pass = properties['eva.evapro.password']
        return pg_url, pg_user, pg_pass

    def get_pg_conn(self):
        pg_url, pg_user, pg_pass = self.get_pg_creds()
        # need to cut the jdbc: from the front of the pg_url
        return psycopg2.connect(pg_url[5:], user=pg_user, password=pg_pass)

    def get_study_name(self):
        with self.get_pg_conn() as conn:
            query = f"SELECT title FROM evapro.project WHERE project_accession='{self.project_accession}';"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            raise ValueError(f'More than one project with accession {self.project_accession} found in metadata DB.')
        return rows[0][0]

    def get_vep_species(self):
        words = self.eload_cfg.query('submission', 'scientific_name').lower().split()
        return '_'.join(words)

    def run_accession_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory(base=self.project_dir)
        accession_config = {
            'project_accession': self.project_accession,
            'accession_props': self.eload_cfg.query(self.config_section, 'accession', 'properties'),
            'public_dir': os.path.join(self.project_dir, project_dirs['public']),
            'logs_dir': os.path.join(self.project_dir, project_dirs['logs']),
            'executable': cfg['executable'],
            'jar': cfg['jar'],
        }
        accession_config_file = os.path.join(self.project_dir, 'accession_config_file.yaml')
        with open(accession_config_file, 'w') as open_file:
            yaml.safe_dump(accession_config, open_file)
        accession_script = os.path.join(ROOT_DIR, 'nextflow', 'ingestion.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow Accessioning process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], accession_script,
                    '-params-file', accession_config_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('Nextflow accessioning pipeline failed: results might not be complete')
            raise e
        return output_dir

    def run_variant_load_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory(base=self.project_dir)
        load_config = {
            'variant_load_props': self.eload_cfg.query(self.config_section, 'variant_load', 'properties'),
            'logs_dir': os.path.join(self.project_dir, project_dirs['logs']),
            'eva_pipeline_props': cfg['eva_pipeline_props'],
            'jar': cfg['jar'],
        }
        load_config_file = os.path.join(self.project_dir, 'load_config_file.yaml')
        with open(load_config_file, 'w') as open_file:
            yaml.safe_dump(load_config, open_file)
        variant_load_script = os.path.join(ROOT_DIR, 'nextflow', 'variant_load.nf')
        try:
            command_utils.run_command_with_output(
                'Nextflow Variant Load process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], variant_load_script,
                    '-params-file', load_config_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('Nextflow variant load pipeline failed: results might not be complete')
            raise e
        return output_dir

    @cached_property
    def valid_vcf_filenames(self):
        return list(self.project_dir.joinpath(project_dirs['valid']).glob('*.vcf.gz'))
