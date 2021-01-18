import subprocess

from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_submission import Eload


class EloadIngestion(Eload):
    config_section = 'ingestion'  # top-level config key
    all_tasks = ['metadata_load', 'accession', 'variant_load']

    def ingest(self, tasks=None):
        ingestion_info = self.eload_cfg.query(self.config_section)
        # TODO what to do if we've already run ingestion? maybe a --forced flag?
        # if len(ingestion_info) > 0:

        if not tasks:
            tasks = self.all_tasks
        self.eload_cfg.set(self.config_section, 'ingestion_date', value=self.now)

        if 'metadata_load' in tasks:
            self.load_from_ena()
        if 'accession' in tasks:
            self.warning('Accessioning not yet supported, skipping.')
        if 'variant_load' in tasks:
            self.warning('Variant loading not yet supported, skipping.')

    def load_from_ena(self):
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        if not project_accession:
            self.error('No project accession in submission config, check that brokering to ENA is done. ')
            raise ValueError('No project accession in submission config')
        try:
            command_utils.run_command_with_output(
                'Load metadata from ENA to EVADEV',
                ' '.join((
                    'perl', cfg['executable']['load_from_ena'],
                    '-p', project_accession,
                    # Current submission process never changes -c or -v
                    '-c', 'submitted',
                    '-v', '1',
                    # -l is only checked for -c=eva_value_added, so in reality never used
                    '-l', self._get_dir('scratch'),
                    '-e', str(self.eload_num)
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('ENA metadata load failed: aborting ingestion.')
            raise e
        # TODO write something to config on success/failure
