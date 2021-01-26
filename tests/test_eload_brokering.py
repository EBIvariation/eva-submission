import glob
import os
import shutil
from unittest import TestCase
from unittest.mock import patch

from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.eload_brokering import EloadBrokering
from eva_submission.eload_submission import Eload
from eva_submission.submission_config import load_config
from tests.test_eload_submission import touch


class TestEloadBrokering(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload = EloadBrokering(3)

    def tearDown(self) -> None:
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_3'))
        for eload in eloads:
            shutil.rmtree(eload)

    def test_run_brokering_prep_workflow(self):
        cfg.content['executable'] = {
            'nextflow': 'path_to_nextflow'
        }
        temp_dir = 'temporary_directory'
        nf_script = os.path.join(ROOT_DIR, 'nextflow', 'prepare_brokering.nf')
        config_file = os.path.join(self.eload.eload_dir, 'brokering_config_file.yaml')

        with patch('eva_submission.eload_brokering.command_utils.run_command_with_output') as m_execute, \
                patch.object(Eload, 'create_nextflow_temp_output_directory', return_value=temp_dir):
            self.eload._run_brokering_prep_workflow()
        m_execute.assert_called_once_with(
            'Nextflow brokering preparation process',
            f'path_to_nextflow {nf_script} -params-file {config_file} -work-dir {temp_dir}'
        )

    def test_collect_brokering_workflow_results(self):
        tmp_dir = os.path.join(self.eload.eload_dir, 'tmp')
        output_dir = os.path.join(tmp_dir, 'output')
        os.makedirs(output_dir)
        for f in ['vcf_file1.vcf.gz']:
            touch(os.path.join(output_dir, f))
        for f in ['vcf_file1.vcf.gz.md5', 'vcf_file1.vcf.gz.tbi', 'vcf_file1.vcf.gz.tbi.md5']:
            touch(os.path.join(tmp_dir, f), content=f'md5checksum {f}')
        self.eload.eload_cfg.set('validation', 'valid', 'vcf_files', value={
            'vcf_file1.vcf': ''
        })
        self.eload._collect_brokering_prep_results(tmp_dir)
        vcf_file1 = os.path.join(self.eload.eload_dir, '18_brokering/ena/vcf_file1.vcf.gz')
        vcf_file1_index = os.path.join(self.eload.eload_dir, '18_brokering/ena/vcf_file1.vcf.gz.tbi')
        assert os.path.isfile(vcf_file1)
        assert os.path.isfile(vcf_file1_index)
        assert self.eload.eload_cfg['brokering']['vcf_files'] == {
            vcf_file1: {
                'original_vcf': 'vcf_file1.vcf',
                'md5': 'md5checksum',
                'index': vcf_file1_index,
                'index_md5': 'md5checksum'
            }
        }
