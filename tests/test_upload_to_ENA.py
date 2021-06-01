from unittest import TestCase

from eva_submission.ENA_submission.upload_to_ENA import ENAUploader


class TestENAUploader(TestCase):

    def setUp(self) -> None:
        self.uploader = ENAUploader('ELOAD_1')

    def test_parse_ena_receipt(self):
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
        assert self.uploader.parse_ena_receipt(receipt) == {
            'errors': [],
            'ANALYSIS': {'ERZ1695006': 'FGV analysis b'},
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
            'errors': 'Cannot parse ENA receipt: This is a random message that cannot be parsed by XML libraries'
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
            'ANALYSIS': {'ERZ1695005': 'FGV analysis a', 'ERZ1695006': 'FGV analysis b'},
            'PROJECT': 'PRJEB42220',
            'SUBMISSION': 'ERA3202812'
        }
