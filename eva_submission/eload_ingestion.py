import os
import shutil
import subprocess
from urllib.parse import urlsplit
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
from pymongo.uri_parser import split_hosts

from eva_submission import ROOT_DIR
from eva_submission.assembly_taxonomy_insertion import insert_new_assembly_and_taxonomy
from eva_submission.eload_submission import Eload
from eva_submission.ingestion_templates import accession_props_template

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
        self.mongo_uri = get_mongo_uri_for_eva_profile(cfg['environment'], self.settings_xml_file)

    def ingest(
            self,
            aggregation=None,
            instance_id=None,
            vep_version=None,
            vep_cache_version=None,
            db_name=None,
            tasks=None
    ):
        self.eload_cfg.set(self.config_section, 'ingestion_date', value=self.now)
        self.check_brokering_done()
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
            # TODO move accession prop creation to nextflow
            accession_prop_files = self.create_accession_properties()
            self.eload_cfg.set(self.config_section, 'accession', 'properties', value=accession_prop_files)
            self.run_accession_workflow()

        if do_variant_load:
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'version', value=vep_version)
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'cache_version', value=vep_cache_version)
            self.run_variant_load_workflow()

    def check_brokering_done(self):
        if self.eload_cfg.query('brokering', 'vcf_files') is None:
            self.error('No brokered VCF files found, aborting ingestion.')
            raise ValueError('No brokered VCF files found.')
        if self.project_accession is None:
            self.error('No project accession in submission config, check that brokering to ENA is done. ')
            raise ValueError('No project accession in submission config.')

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
        If db_name is omitted, looks up the name in metadata DB and checks mongo for that.
        If db_name is provided, it will also attempt to insert into the metadata DB before checking mongo.
        """
        if not db_name:
            db_name = self.get_db_name()
        else:
            with self.get_pg_conn() as conn:
                # warns but doesn't crash if assembly set already exists
                insert_new_assembly_and_taxonomy(
                    assembly_accession=self.eload_cfg.query('submission', 'assembly_accession'),
                    taxonomy_id=self.eload_cfg.query('submission', 'taxonomy_id'),
                    db_name=db_name,
                    conn=conn
                )
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
        for v in project_dirs.values():
            os.makedirs(project_dir.joinpath(v), exist_ok=True)
        # copy valid vcfs + index to 'valid' folder
        valid_dir = project_dir.joinpath(project_dirs['valid'])
        vcf_dict = self.eload_cfg.query('brokering', 'vcf_files')
        for key, val in vcf_dict.items():
            vcf_path = Path(key)
            shutil.copyfile(vcf_path, valid_dir.joinpath(vcf_path.name))
            index_path = Path(val['index'])
            shutil.copyfile(index_path, valid_dir.joinpath(index_path.name))
        self.eload_cfg.set(self.config_section, 'project_dir', value=str(project_dir))
        return project_dir

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

            properties_filename = self.project_dir.joinpath(
                project_dirs['accessions'],
                f'{filename}_accessioning.properties'
            )
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

    def get_load_job_properties(self):
        """
        Get all properties needed for this variant load job, except for the vcf file
        which is filled in after (optional) merge.
        """
        aggregation = self.eload_cfg.query(self.config_section, 'aggregation')
        vep_version = self.eload_cfg.query(self.config_section, 'variant_load', 'vep', 'version')
        return {
            'spring.batch.job.names': 'genotyped-vcf-job' if aggregation == 'none' else 'aggregated-vcf-job',
            'input.study.id': self.project_accession,
            'input.vcf.id': self.eload_cfg.query('brokering', 'ena', 'ANALYSIS'),
            'input.vcf.aggregation': aggregation,
            'input.study.name': self.get_study_name(),
            'input.study.type': 'COLLECTION',
            'input.fasta': self.eload_cfg.query('submission', 'assembly_fasta'),
            'output.dir': str(self.project_dir.joinpath(project_dirs['transformed'])),
            'output.dir.annotation': str(self.project_dir.joinpath(project_dirs['annotation'])),
            'output.dir.statistics': str(self.project_dir.joinpath(project_dirs['stats'])),
            'spring.data.mongodb.database': self.eload_cfg.query(self.config_section, 'database', 'db_name'),
            'db.collections.files.name': 'files_2_0',
            'db.collections.variants.name': 'variants_2_0',
            'db.collections.annotation-metadata.name': 'annotationMetadata_2_0',
            'db.collections.annotations.name': 'annotations_2_0',
            'app.vep.version': vep_version,
            'app.vep.path': f"{cfg['vep_path']}/ensembl-vep-release-{vep_version}/vep",
            'app.vep.cache.version': self.eload_cfg.query(self.config_section, 'variant_load', 'vep', 'cache_version'),
            'app.vep.cache.path': cfg['vep_cache_path'],
            'app.vep.cache.species': self.get_vep_species(),
            'app.vep.num-forks': 4,
            'app.vep.timeout': 500,
            'statistics.skip': False,
            'annotation.skip': False,
            'annotation.overwrite': False,
            'config.chunk.size': 200,
        }

    def get_mongo_creds(self):
        properties = get_properties_from_xml_file(cfg['environment'], self.settings_xml_file)
        mongo_host = split_hosts(properties['eva.mongo.host'])[0][0]
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
        return psycopg2.connect(urlsplit(pg_url).path, user=pg_user, password=pg_pass)

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
            self.error('See .nextflow.log for more details')
            raise e
        return output_dir

    def run_variant_load_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory(base=self.project_dir)
        load_config = {
            'valid_vcfs': [str(f) for f in self.valid_vcf_filenames],
            # TODO implement proper merge check or get from validation
            'needs_merge': self.eload_cfg.query(self.config_section, 'aggregation') == 'none',
            'job_props': self.get_load_job_properties(),
            'project_accession': self.project_accession,
            'logs_dir': os.path.join(self.project_dir, project_dirs['logs']),
            'eva_pipeline_props': cfg['eva_pipeline_props'],
            'executable': cfg['executable'],
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
            self.error('See .nextflow.log for more details')
            raise e
        return output_dir

    @cached_property
    def valid_vcf_filenames(self):
        return list(self.project_dir.joinpath(project_dirs['valid']).glob('*.vcf.gz'))
