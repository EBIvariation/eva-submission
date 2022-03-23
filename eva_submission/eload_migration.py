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
        # Load the copied config file
        self.eload_cfg.load_config_file(self.config_path)
        self.update_config_paths()

    def run_nextflow_copy(self, project_accession):
        params = {
            'eload': self.eload,
            'old_submissions_dir': cfg['old_eloads_dir'],
            'new_submissions_dir': cfg['eloads_dir'],
            'old_projects_dir': cfg['old_projects_dir'],
            'new_projects_dir': cfg['projects_dir'],
        }
        if project_accession:
            params['project_accession'] = project_accession
        work_dir = self.create_nextflow_temp_output_directory()
        params_file = os.path.join(self.eload_dir, 'migrate_params.yaml')
        log_file = os.path.join(self.eload_dir, 'migrate_nextflow.log')

        with open(params_file, 'w') as open_file:
            yaml.safe_dump(params, open_file)
        nextflow_script = os.path.join(NEXTFLOW_DIR, 'migrate_to_codon.nf')

        try:
            command_utils.run_command_with_output(
                f'Nextflow migrate process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], nextflow_script,
                    '-params-file', params_file,
                    '-log', log_file,
                    '-work-dir', work_dir.name
                ))
            )
            shutil.rmtree(work_dir)
        except subprocess.CalledProcessError as e:
            raise e

    def update_config_paths(self):
        pass
