import os
from unittest import TestCase
from unittest.mock import patch

from eva_submission.eload_backlog import EloadBacklog
from eva_submission.submission_config import load_config


class TestEloadBacklog(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload = EloadBacklog(44)

    def tearDown(self):
        # necessary because test instances are retained during a run and content is a class variable
        from eva_submission.submission_config import EloadConfig
        EloadConfig.content = {}
        # forces the eload config to be written and hence deleted
        del self.eload
        os.remove(os.path.join(self.resources_folder, 'eloads/ELOAD_44/.ELOAD_44_config.yml'))

    def test_fill_in_config(self):
        expected_vcf = os.path.join(self.resources_folder, 'eloads/ELOAD_44/10_submitted/vcf_files/file.vcf')
        expected_index = os.path.join(self.resources_folder, 'eloads/ELOAD_44/10_submitted/vcf_files/file.vcf.tbi')
        expected_config = {
            'submission': {
                'vcf_files': [expected_vcf],
                'assembly_fasta': 'assembly.fa',
                'assembly_report': 'assembly.txt',
                'assembly_accession': 'GCA_000003025.4',
                'scientific_name': 'Sus scrofa',
                'taxonomy_id': 9823,
            },
            'brokering': {
                'vcf_files': {expected_vcf: {'index': expected_index}},
                'ena': {
                    'hold_date':  '2021-01-01+01:00',
                    'ANALYSIS': 'ERZ999999',
                    'PROJECT': 'PRJEB12345',
                }
            }
        }
        with patch('eva_submission.eload_backlog.get_metadata_conn', autospec=True), \
                patch('eva_submission.eload_backlog.get_all_results_for_query') as m_get_results, \
                patch('eva_submission.eload_backlog.get_reference_fasta_and_report') as m_get_genome, \
                patch('eva_submission.eload_utils.get_metadata_conn', autospec=True), \
                patch('eva_submission.eload_utils.get_all_results_for_query') as m_get_alias_results, \
                patch('eva_submission.eload_utils.requests.post') as m_post:
            m_get_alias_results.return_value = [['alias']]
            m_get_results.side_effect = [
                [['PRJEB12345']],
                [('ERZ999999', ('file.vcf', 'file.vcf.tbi'))],
                [(9823, 'Sus scrofa', 'GCA_000003025.4')],
            ]
            m_get_genome.return_value = ('assembly.fa', 'assembly.txt')
            m_post.return_value.text = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
<RECEIPT receiptDate="2021-04-19T18:37:45.129+01:00" submissionFile="SUBMISSION" success="true">
     <ANALYSIS accession="ERZ999999" alias="MD" status="PRIVATE"/>
     <PROJECT accession="PRJEB12345" alias="alias" status="PRIVATE" holdUntilDate="2021-01-01+01:00"/>
     <SUBMISSION accession="ERA3972426" alias="alias"/>
     <MESSAGES/>
     <ACTIONS>RECEIPT</ACTIONS>
</RECEIPT>'''
            self.eload.fill_in_config()
            self.assertEqual(self.eload.eload_cfg.content, expected_config)

    def test_file_not_found(self):
        expected_config = {
            'brokering': {
                'ena': {
                    'ANALYSIS': 'ERZ999999',
                    'PROJECT': 'PRJEB12345',
                }
            }
        }
        with patch('eva_submission.eload_backlog.get_metadata_conn', autospec=True), \
                patch('eva_submission.eload_backlog.get_all_results_for_query') as m_get_results:
            m_get_results.side_effect = [
                [['PRJEB12345']],
                [('ERZ999999', ('something_else.vcf', 'file.vcf.tbi'))]
            ]
            with self.assertRaises(FileNotFoundError):
                self.eload.fill_in_config()
            # incomplete config should still exist, even though filling config failed
            self.eload = EloadBacklog(44)
            self.assertEqual(self.eload.eload_cfg.content, expected_config)
