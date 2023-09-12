from unittest import TestCase
import os
from unittest.mock import patch, Mock

import pytest
from ebi_eva_common_pyutils.config import cfg

from eva_submission import ROOT_DIR
from eva_submission.ENA_submission.upload_to_ENA import ENAUploader, ENAUploaderAsync, HackFTP_TLS
from eva_submission.eload_utils import get_file_content
from eva_submission.submission_config import load_config


class TestENAUploader(TestCase):
    receipt = '''<?xml version="1.0" encoding="UTF-8"?>
    <?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
    <RECEIPT receiptDate="2020-12-21T16:23:42.950Z" submissionFile="ELOAD_733.Submission.xml" success="true">
         <ANALYSIS accession="ERZ1695006" alias="FGV analysis b" status="PRIVATE"/>
         <PROJECT accession="PRJEB42220" alias="ICFADS2b" status="PRIVATE" holdUntilDate="2022-12-21Z">
              <EXT_ID accession="ERP126058" type="study"/>
         </PROJECT>
         <SUBMISSION accession="ERA3202812" alias="ICFADS2b"/>
         <MESSAGES>
              <INFO>Submission has been committed.</INFO>
         </MESSAGES>
         <ACTIONS>ADD</ACTIONS>
         <ACTIONS>ADD</ACTIONS>
    </RECEIPT>'''


    def setUp(self) -> None:
        self.resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')
        self.brokering_folder = os.path.join(self.resources_folder, 'brokering')

        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        metadata_file = os.path.join(self.brokering_folder, 'metadata_sheet.xlsx')
        self.uploader = ENAUploader('ELOAD_1', metadata_file, self.brokering_folder)
        self.uploader_async = ENAUploaderAsync('ELOAD_1', metadata_file, self.brokering_folder)

    def tearDown(self) -> None:
        if os.path.exists(self.uploader_async.converter.single_submission_file):
            os.remove(self.uploader_async.converter.single_submission_file)

    def test_parse_ena_receipt(self):
        assert self.uploader.parse_ena_receipt(self.receipt) == {
            'errors': [],
            'ANALYSIS': {'FGV analysis b': 'ERZ1695006'},
            'PROJECT': 'PRJEB42220',
            'SUBMISSION': 'ERA3202812'
        }
        receipt = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
<RECEIPT receiptDate="2020-10-29T10:48:02.303Z" submissionFile="ELOAD_697.Submission.xml" success="false">
     <SUBMISSION alias="Sorghum GBS SNPs"/>
     <MESSAGES>
          <ERROR>In submission, alias:"Sorghum GBS SNPs", accession:"". The object being added already exists in the submission account with accession: "ERA3030993".</ERROR>
          <INFO>Submission has been rolled back.</INFO>
     </MESSAGES>
     <ACTIONS>ADD</ACTIONS>
     <ACTIONS>ADD</ACTIONS>
</RECEIPT>'''
        assert self.uploader.parse_ena_receipt(receipt) == {
            'errors': ['In submission, alias:"Sorghum GBS SNPs", accession:"". The object being added already exists in the submission account with accession: "ERA3030993".']
        }

    def test_parse_ena_receipt_failed(self):
        receipt = '''This is a random message that cannot be parsed by XML libraries'''
        assert self.uploader.parse_ena_receipt(receipt) == {
            'errors': ['Cannot parse ENA receipt: This is a random message that cannot be parsed by XML libraries']
        }

    def test_parse_ena_receipt_multiple_analyses(self):
        receipt = '''<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
<RECEIPT receiptDate="2020-12-21T16:23:42.950Z" submissionFile="ELOAD_733.Submission.xml" success="true">
     <ANALYSIS accession="ERZ1695005" alias="FGV analysis a" status="PRIVATE"/>
     <ANALYSIS accession="ERZ1695006" alias="FGV analysis b" status="PRIVATE"/>
     <PROJECT accession="PRJEB42220" alias="ICFADS2b" status="PRIVATE" holdUntilDate="2022-12-21Z">
          <EXT_ID accession="ERP126058" type="study"/>
     </PROJECT>
     <SUBMISSION accession="ERA3202812" alias="ICFADS2b"/>
     <MESSAGES>
          <INFO>Submission has been committed.</INFO>
     </MESSAGES>
     <ACTIONS>ADD</ACTIONS>
     <ACTIONS>ADD</ACTIONS>
</RECEIPT>'''

        assert self.uploader.parse_ena_receipt(receipt) == {
            'errors': [],
            'ANALYSIS': {'FGV analysis a': 'ERZ1695005', 'FGV analysis b': 'ERZ1695006'},
            'PROJECT': 'PRJEB42220',
            'SUBMISSION': 'ERA3202812'
        }

    def test_single_upload_xml_files_to_ena(self):
        with patch.object(ENAUploader, '_post_xml_file_to_ena') as mock_post,\
             patch('eva_submission.ENA_submission.upload_to_ENA.requests.get') as mock_get:
            json_data = {'submissionId': 'ERA123456', '_links': [{'rel': 'poll-xml', 'href': 'https://example.com/link'}]}
            mock_post.return_value = Mock(status_code=200, json=Mock(return_value=json_data))
            mock_get.return_value = Mock(status_code=200, text=self.receipt)
            self.assertFalse(os.path.isfile(self.uploader_async.converter.single_submission_file))
            self.uploader_async.upload_xml_files_to_ena()
            self.assertTrue(os.path.isfile(self.uploader_async.converter.single_submission_file))
            mock_post.assert_called_with(
                'https://wwwdev.ebi.ac.uk/ena/submit/webin-v2/submit/queue',
                {'file': (
                    'ELOAD_1.SingleSubmission.xml',
                    get_file_content(self.uploader_async.converter.single_submission_file),
                    'application/xml'
                )}
            )
            mock_get.assert_called_once_with('https://example.com/link', auth=self.uploader_async.ena_auth)
            self.assertEqual(self.uploader_async.results, {
                'submissionId': 'ERA123456', 'poll-links': 'https://example.com/link', 'errors': [],
                'ANALYSIS': {'FGV analysis b': 'ERZ1695006'}, 'PROJECT': 'PRJEB42220', 'SUBMISSION': 'ERA3202812'
            })

    def test_single_upload_xml_files_to_ena_failed(self):
        self.assertFalse(os.path.isfile(self.uploader_async.converter.single_submission_file))
        self.uploader_async.upload_xml_files_to_ena()
        self.assertTrue(os.path.isfile(self.uploader_async.converter.single_submission_file))
        self.assertEqual(self.uploader_async.results, {'errors': ['403']})

    def test_single_dry_upload_xml_files_to_ena(self):
        with patch.object(ENAUploader, '_post_xml_file_to_ena') as mock_post,\
             patch('eva_submission.ENA_submission.upload_to_ENA.requests.get') as mock_get, \
             patch.object(ENAUploaderAsync, 'info') as mock_info:
            self.assertFalse(os.path.isfile(self.uploader_async.converter.single_submission_file))
            self.uploader_async.upload_xml_files_to_ena(dry_ena_upload=True)
            self.assertTrue(os.path.isfile(self.uploader_async.converter.single_submission_file))
            mock_info.assert_any_call('Would have uploaded the following XML files to ENA asynchronous submission '
                                      'endpoint:')
            mock_info.assert_any_call('file: ELOAD_1.SingleSubmission.xml')
            mock_post.assert_not_called()
            mock_get.assert_not_called()

    @pytest.mark.skip(reason="Need to use real ENA credentials in submission_config.yml for this test to work")
    def test_upload_FTP(self):
        filename = 'test_vcf_file.vcf'
        with HackFTP_TLS() as ftps:
            # Connect to delete the file
            ftps.context.set_ciphers('DEFAULT:@SECLEVEL=1')
            ftps.connect(cfg.query('ena', 'ftphost'), port=int(cfg.query('ena', 'ftpport', ret_default=21)))
            ftps.login(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
            ftps.prot_p()
            list_files = ftps.nlst()
            if self.uploader.eload in list_files:
                ftps.cwd(self.uploader.eload)
                list_files = ftps.nlst()
                if self.uploader.eload in list_files:
                    ftps.delete(filename)

        files_to_upload = os.path.join(self.brokering_folder, filename)
        self.uploader.upload_vcf_files_to_ena_ftp([files_to_upload])

        with HackFTP_TLS() as ftps:
            # Connect to check that the file has been uploaded
            ftps.context.set_ciphers('DEFAULT:@SECLEVEL=1')
            ftps.connect(cfg.query('ena', 'ftphost'), port=int(cfg.query('ena', 'ftpport', ret_default=21)))
            ftps.login(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
            ftps.prot_p()
            list_files = ftps.nlst()
            assert self.uploader.eload in list_files
            ftps.cwd(self.uploader.eload)
            list_files = ftps.nlst()
            assert filename in list_files

        # Attempt to load again: It should not load the file
        with patch.object(ENAUploader, 'warning') as mock_warning:
            self.uploader.upload_vcf_files_to_ena_ftp([files_to_upload])
            mock_warning.assert_called_once_with(
                'test_vcf_file.vcf Already exist and has the same size on the FTP. Skip upload.'
            )
