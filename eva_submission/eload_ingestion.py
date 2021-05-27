import os
import shutil
import subprocess
from pathlib import Path

import yaml
from cached_property import cached_property
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_mongo_uri_for_eva_profile
from ebi_eva_common_pyutils.metadata_utils import get_variant_warehouse_db_name_from_assembly_and_taxonomy
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query, execute_query
import pymongo

from eva_submission import NEXTFLOW_DIR
from eva_submission.assembly_taxonomy_insertion import insert_new_assembly_and_taxonomy
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import get_metadata_conn, get_mongo_creds, get_accession_pg_creds, \
    get_hold_date_from_ena
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
        self.project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        self.project_dir = self.setup_project_dir()
        self.mongo_uri = get_mongo_uri_for_eva_profile(cfg['maven']['environment'], cfg['maven']['settings_file'])

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
        self.update_config_with_hold_date()
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
            self.run_accession_workflow()
            self.insert_browsable_files()
            self.refresh_study_browser()

        if do_variant_load:
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'version', value=vep_version)
            self.eload_cfg.set(self.config_section, 'variant_load', 'vep', 'cache_version', value=vep_cache_version)
            self.run_variant_load_workflow()

    def update_config_with_hold_date(self):
        hold_date = get_hold_date_from_ena(self.project_accession)
        self.eload_cfg.set('brokering', 'ena', 'hold_date', value=hold_date)

    def check_brokering_done(self):
        if self.eload_cfg.query('brokering', 'vcf_files') is None:
            self.error('No brokered VCF files found, aborting ingestion.')
            raise ValueError('No brokered VCF files found.')
        if self.project_accession is None:
            self.error('No project accession in submission config, check that brokering to ENA is done. ')
            raise ValueError('No project accession in submission config.')
        if self.eload_cfg.query('brokering', 'ena', 'hold_date') is None:
            self.error('No release date found, check that brokering to ENA is done.')
            raise ValueError('No release date found in submission config.')
        # check there are no vcfs in valid folder that aren't in brokering config
        for valid_vcf in self.valid_vcf_filenames:
            if not any(f.endswith(valid_vcf.name) for f in self.eload_cfg.query('brokering', 'vcf_files').keys()):
                raise ValueError(f'Found {valid_vcf} in valid folder that was not in brokering config')

    def get_db_name(self):
        """
        Constructs the expected database name in mongo, based on assembly info retrieved from EVAPRO.
        """
        assm_accession = self.eload_cfg.query('submission', 'assembly_accession')
        taxon_id = self.eload_cfg.query('submission', 'taxonomy_id')
        # query EVAPRO for db name based on taxonomy id and accession
        with get_metadata_conn() as conn:
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
            with get_metadata_conn() as conn:
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

    def _copy_file(self, source_path, target_dir):
        target_path = target_dir.joinpath(source_path.name)
        if not target_path.exists():
            shutil.copyfile(source_path, target_path)
        else:
            self.warning(f'{source_path.name} already exists in {target_dir}, not copying.')

    def setup_project_dir(self):
        """
        Sets up project directory and copies VCF files from the eload directory.
        """
        project_dir = Path(cfg['projects_dir'], self.project_accession)
        os.makedirs(project_dir, exist_ok=True)
        for v in project_dirs.values():
            os.makedirs(project_dir.joinpath(v), exist_ok=True)
        # copy valid vcfs + index to 'valid' folder and 'public' folder
        valid_dir = project_dir.joinpath(project_dirs['valid'])
        public_dir = project_dir.joinpath(project_dirs['public'])
        vcf_dict = self.eload_cfg.query('brokering', 'vcf_files')
        for key, val in vcf_dict.items():
            vcf_path = Path(key)
            self._copy_file(vcf_path, valid_dir)
            self._copy_file(vcf_path, public_dir)
            tbi_path = Path(val['index'])
            self._copy_file(tbi_path, valid_dir)
            self._copy_file(tbi_path, public_dir)
            try:
                csi_path = Path(val['csi'])
                self._copy_file(csi_path, valid_dir)
                self._copy_file(csi_path, public_dir)
            # for now this won't be available for older studies, we can remove the try/except at a later date
            except KeyError:
                self.warning('No csi filepath found in config, will not make a csi index public.')
        self.eload_cfg.set(self.config_section, 'project_dir', value=str(project_dir))
        return project_dir

    def get_study_name(self):
        with get_metadata_conn() as conn:
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
        mongo_host, mongo_user, mongo_pass = get_mongo_creds()
        pg_url, pg_user, pg_pass = get_accession_pg_creds()
        job_props = accession_props_template(
            assembly_accession=self.eload_cfg.query('submission', 'assembly_accession'),
            taxonomy_id=self.eload_cfg.query('submission', 'taxonomy_id'),
            project_accession=self.project_accession,
            aggregation=self.eload_cfg.query(self.config_section, 'aggregation'),
            fasta=self.eload_cfg.query('submission', 'assembly_fasta'),
            report=self.eload_cfg.query('submission', 'assembly_report'),
            instance_id=self.eload_cfg.query(self.config_section, 'accession', 'instance_id'),
            mongo_host=mongo_host,
            mongo_user=mongo_user,
            mongo_pass=mongo_pass,
            postgres_url=pg_url,
            postgres_user=pg_user,
            postgres_pass=pg_pass
        )
        accession_config = {
            'valid_vcfs': [str(f) for f in self.valid_vcf_filenames],
            'project_accession': self.project_accession,
            'instance_id': self.eload_cfg.query(self.config_section, 'accession', 'instance_id'),
            'accession_job_props': job_props,
            'public_ftp_dir': cfg['public_ftp_dir'],
            'accessions_dir': os.path.join(self.project_dir, project_dirs['accessions']),
            'public_dir': os.path.join(self.project_dir, project_dirs['public']),
            'logs_dir': os.path.join(self.project_dir, project_dirs['logs']),
            'executable': cfg['executable'],
            'jar': cfg['jar'],
        }
        accession_config_file = os.path.join(self.project_dir, 'accession_config_file.yaml')
        with open(accession_config_file, 'w') as open_file:
            yaml.safe_dump(accession_config, open_file)
        accession_script = os.path.join(NEXTFLOW_DIR, 'accession.nf')
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
            self.error('Nextflow accessioning pipeline failed: results might not be complete.')
            self.error(f"See Nextflow logs in {self.eload_dir}/.nextflow.log or accessioning logs "
                       f"in {self.project_dir.joinpath(project_dirs['logs'])} for more details.")
            raise e
        return output_dir

    def run_variant_load_workflow(self):
        output_dir = self.create_nextflow_temp_output_directory(base=self.project_dir)
        job_props = variant_load_props_template(
                project_accession=self.project_accession,
                # TODO currently there is only ever one of these in the config, even if multiple analyses/files
                analysis_accession=self.eload_cfg.query('brokering', 'ena', 'ANALYSIS'),
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
        )
        load_config = {
            'valid_vcfs': [str(f) for f in self.valid_vcf_filenames],
            # TODO implement proper merge check or get from validation
            'needs_merge': self.needs_merge,
            'load_job_props': job_props,
            'project_accession': self.project_accession,
            'project_dir': str(self.project_dir),
            'logs_dir': os.path.join(self.project_dir, project_dirs['logs']),
            'eva_pipeline_props': cfg['eva_pipeline_props'],
            'executable': cfg['executable'],
            'jar': cfg['jar'],
        }
        load_config_file = os.path.join(self.project_dir, 'load_config_file.yaml')
        with open(load_config_file, 'w') as open_file:
            yaml.safe_dump(load_config, open_file)
        variant_load_script = os.path.join(NEXTFLOW_DIR, 'variant_load.nf')
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
            self.error(f"See Nextflow logs in {self.eload_dir}/.nextflow.log or pipeline logs "
                       f"in {self.project_dir.joinpath(project_dirs['logs'])} for more details.")
            raise e
        return output_dir

    def insert_browsable_files(self):
        with get_metadata_conn() as conn:
            # insert into browsable file table, if files not already there
            files_query = f"select file_id, filename from evapro.browsable_file " \
                          f"where project_accession = '{self.project_accession}';"
            rows = get_all_results_for_query(conn, files_query)
            if len(rows) > 0:
                self.info('Browsable files already inserted, skipping')
                return
            self.info('Inserting browsable files...')
            insert_query = "insert into browsable_file (file_id,ena_submission_file_id,filename,project_accession,assembly_set_id) " \
                           "select file.file_id,ena_submission_file_id,filename,project_accession,assembly_set_id " \
                           "from (select * from analysis_file af " \
                           "join analysis a on a.analysis_accession = af.analysis_accession " \
                           "join project_analysis pa on af.analysis_accession = pa.analysis_accession " \
                           f"where pa.project_accession = '{self.project_accession}' ) myfiles " \
                           "join file on file.file_id = myfiles.file_id where file.file_type ilike 'vcf';"
            execute_query(conn, insert_query)

            # update loaded and release date
            # TODO get release date from ENA directly
            release_date = self.eload_cfg.query('brokering', 'ena', 'hold_date')
            release_update = f"update evapro.browsable_file " \
                             f"set loaded = true, eva_release = '{release_date.strftime('%Y%m%d')}' " \
                             f"where project_accession = '{self.project_accession}';"
            execute_query(conn, release_update)

            # update FTP file paths
            rows = get_all_results_for_query(conn, files_query)
            if len(rows) == 0:
                raise ValueError('Something went wrong with loading from ENA')
            for file_id, filename in rows:
                ftp_update = f"update evapro.file " \
                             f"set ftp_file = '/ftp.ebi.ac.uk/pub/databases/eva/{self.project_accession}/{filename}' " \
                             f"where file_id = '{file_id}';"
                execute_query(conn, ftp_update)

    def refresh_study_browser(self):
        with get_metadata_conn() as conn:
            execute_query(conn, 'refresh materialized view study_browser;')

    @cached_property
    def needs_merge(self):
        return len(self.valid_vcf_filenames) > 1 and self.eload_cfg.query(self.config_section, 'aggregation') == 'none'

    @cached_property
    def valid_vcf_filenames(self):
        return list(self.project_dir.joinpath(project_dirs['valid']).glob('*.vcf.gz'))
