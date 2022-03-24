import os
import shutil
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_submission import Eload


class EloadMigration(Eload):

    def migrate(self, project_accession=None):
        self.run_nextflow_copy(project_accession)
        self.update_and_reload_config()

    def run_nextflow_copy(self, project_accession=None):
        migrate_params = {
            'eload': self.eload,
            'old_eloads_dir': cfg['old_eloads_new_mnt'],
            'new_eloads_dir': cfg['eloads_dir'],
            'old_projects_dir': cfg['old_projects_new_mnt'],
            'new_projects_dir': cfg['projects_dir'],
        }
        if project_accession:
            migrate_params['project_accession'] = project_accession
        work_dir = self.create_nextflow_temp_output_directory()
        params_file = os.path.join(self.eload_dir, 'migrate_params.yaml')
        # Use a specific log file so we don't overwrite when we sync
        log_file = os.path.join(self.eload_dir, 'migrate_nextflow.log')

        with open(params_file, 'w') as open_file:
            yaml.safe_dump(migrate_params, open_file)
        nextflow_script = os.path.join(NEXTFLOW_DIR, 'migrate.nf')

        try:
            command_utils.run_command_with_output(
                f'Nextflow migrate process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], '-log', log_file,
                    'run', nextflow_script,
                    '-params-file', params_file,
                    '-work-dir', work_dir
                ))
            )
            shutil.rmtree(work_dir)
        except subprocess.CalledProcessError as e:
            raise e

    def update_and_reload_config(self):
        if not os.path.exists(self.config_path):
            return
        with open(self.config_path, 'r') as config_file:
            config_contents = config_file.read()
        config_contents = config_contents\
            .replace(cfg['genome_downloader']['old_output_directory'], cfg['genome_downloader']['output_directory'])\
            .replace(cfg['old_eloads_dir'], cfg['eloads_dir'])\
            .replace(cfg['old_projects_dir'], cfg['projects_dir'])

        with open(self.config_path, 'w') as config_file:
            config_file.write(config_contents)
        # Re-load the copied and modified config
        self.eload_cfg.load_config_file(self.config_path)
