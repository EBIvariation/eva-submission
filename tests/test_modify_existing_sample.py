import json
import os
from unittest import TestCase

from bin.modify_existing_sample import XlsxExistingSampleParser
from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_submission.eload_utils import convert_spreadsheet_to_json


class TestModifyExistingSample(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        self.metadata_xlsx = os.path.join(self.resources_folder, 'brokering', 'EVA_Submission_metadata_to_modify_sample.xlsx')
        self.metadata_json_file_path = os.path.join( self.resources_folder, 'brokering', 'EVA_Submission_metadata.json')

    def tearDown(self):
        if os.path.exists(self.metadata_json_file_path):
            os.remove(self.metadata_json_file_path)

    def test_convert_all_fields_in_bioSamples(self):
        convert_spreadsheet_to_json(self.metadata_xlsx, self.metadata_json_file_path, xls_parser=XlsxExistingSampleParser)
        with open(self.metadata_json_file_path) as open_file:
            metadata_json = json.load(open_file)
            assert metadata_json['sample'][0] == {
                'analysisAlias': ['HGV analysis'],
                'sampleInVCF': 'sample_A',
                'bioSampleAccession': 'SAMEA000000000',
                'bioSampleObject': {
                    'characteristics': {
                        'bioSampleAccession': [{'text': 'SAMEA000000000'}],
                        'title': [{'text': 'A human sample'}],
                        'description': [{'text': 'A human sample tested under condition X'}],
                        'subject': [{'text': 'sample_B'}],
                        'taxId': [{'text': '9606'}],
                        'scientific name': [{'text': 'Homo sapiens'}],
                        'commonName': [{'text': 'Human'}],
                        'sex': [{'text': 'Male'}],
                        'population': [{'text': 'Welsh'}],
                        'collected_by': [{'text': 'Joe Bloggs'}],
                        'collection date': [{'text': '2025-02-01'}],
                        'geographic location (country and/or sea)': [{'text': 'United Kingdom'}],
                        'geographic location (region and locality)': [{'text': 'Wales'}],
                        'species': [{'text': 'Homo sapiens'}]
                    },
                    'name': 'my_public_sample_name'
                }
            }

    def test_convert_to_biosamples_for_derive(self):
        convert_spreadsheet_to_json(self.metadata_xlsx, self.metadata_json_file_path, xls_parser=XlsxExistingSampleParser)
        with open(self.metadata_json_file_path) as open_file:
            metadata_json = json.load(open_file)
            sample_submitter = SampleJSONSubmitter(metadata_json, submit_type=('derive',))
            source_sample_json, sample_name_from_metadata, sample_accession = next(sample_submitter._convert_metadata())
            assert source_sample_json['accession'] == 'SAMEA000000000'
            assert sample_accession is None
