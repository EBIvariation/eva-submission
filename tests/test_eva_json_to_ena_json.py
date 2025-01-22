import json
import os
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch, PropertyMock

from eva_submission import ROOT_DIR
from eva_submission.ENA_submission.json_to_ENA_json import EnaJsonConverter


class TestEVAJsonToENAJsonConverter(TestCase):

    def setUp(self) -> None:
        self.brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        self.metadata_file = os.path.join(self.brokering_folder, 'eva_metadata_json.json')
        self.converter = EnaJsonConverter('Submission-12345', self.metadata_file, self.brokering_folder,
                                          'To_Json_Converter_Test')

        self.project = {
            "title": "Example Project",
            "description": "An example project for demonstration purposes",
            "centre": "University of Example",
            "taxId": 9606,
            "parentProject": "PRJEB00001",
            "childProjects": ["PRJEB00002", "PRJEB00003"],
            "peerProjects": ["PRJEB00004", "PRJEB00005"],
            "publications": ["PubMed:123456", "PubMed:789012"],
            "links": ["http://www.abc.com|abc", "http://xyz.com", "PubMed:123456", "PubMed:789012:abcxyz"]
        }

        self.analysis = {
            'analysisTitle': 'Genomic Relationship Matrix',
            'analysisAlias': 'GRM',
            'description': 'A genomic relationship matrix (GRM) was computed',
            'Project Title': 'TechFish - Vibrio challenge',
            'experimentType': 'Genotyping by array',
            'referenceGenome': 'http://abc.com',
            'software': 'software package GCTA, Burrows-Wheeler Alignment tool (BWA), HTSeq-python package ',
            'platform': "BGISEQ-500",
            'imputation': "1",
            "links": ["http://www.abc.com|abc", "http://xyz.com", "PubMed:123456", "PubMed:789012:abcxyz"],

        }

        self.samples = [
            {'analysisAlias': 'GRM', 'sampleId': '201903VIBRIO1185679118', 'bioSampleAccession': 'SAMEA7851610'},
            {'analysisAlias': 'GRM', 'sampleId': '201903VIBRIO1185679119', 'bioSampleAccession': 'SAMEA7851611'},
            {'analysisAlias': 'GRM', 'sampleId': '201903VIBRIO1185679120', 'bioSampleAccession': 'SAMEA7851612'},
            {'analysisAlias': 'GRM', 'sampleId': '201903VIBRIO1185679121', 'bioSampleAccession': 'SAMEA7851613'},
            {'analysisAlias': 'GRM', 'sampleId': '201903VIBRIO1185679122', 'bioSampleAccession': 'SAMEA7851614'},
        ]

        self.files = [
            {'analysisAlias': 'GRM', 'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz',
             'fileType': 'vcf', 'md5': 'c263a486e9b273d6e1e4c5f46ca5ccb8'},
            {'analysisAlias': 'GRM', 'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi',
             'fileType': 'tabix', 'md5': '4b61e00524cc1f4c98e932b0ee27d94e'},
        ]

    @staticmethod
    def _delete_file(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)

    def tearDown(self) -> None:
        self._delete_file(os.path.join(self.brokering_folder, 'To_Json_Converter_Test.json'))

    def test_create_ena_project_json_obj(self):
        expected_project_json_obj = {
            'accession': None,
            'alias': 'Submission-12345',
            'title': 'Example Project',
            'description': 'An example project for demonstration purposes',
            'centre': 'University of Example',
            'publicationLinks': [
                {'xrefLink': {'db': 'PubMed', 'id': '123456'}},
                {'xrefLink': {'db': 'PubMed', 'id': '789012'}}
            ],
            'sequencingProject': {},
            'organism': {'taxonId': 9606, 'scientificName': 'Oncorhynchus mykiss'},
            'relatedProjects': [
                {'parentProject': 'PRJEB00001'},
                {'childProject': 'PRJEB00002'},
                {'childProject': 'PRJEB00003'},
                {'peerProject': 'PRJEB00004'},
                {'peerProject': 'PRJEB00005'}
            ],
            'projectLinks': [
                {'urlLink': {'label': 'abc', 'url': 'http://www.abc.com'}},
                {'urlLink': {'label': 'http://xyz.com', 'url': 'http://xyz.com'}},
                {'xrefLink': {'db': 'PubMed', 'id': '123456'}},
                {'xrefLink': {'db': 'PubMed', 'id': '789012', 'label': 'abcxyz'}}
            ]
        }

        with patch('eva_submission.ENA_submission.json_to_ENA_json.get_scientific_name_from_ensembl') as m_sci_name:
            m_sci_name.return_value = 'Oncorhynchus mykiss'
            ena_project_json_obj = self.converter._create_ena_project_json_obj(self.project,
                                                                               "Submission-12345")
            self.assert_json_equal(expected_project_json_obj, ena_project_json_obj)

    def test_add_analysis(self):
        expected_analysis_json_obj = {
            'title': 'Genomic Relationship Matrix',
            'description': 'A genomic relationship matrix (GRM) was computed',
            'centre': 'University of Example',
            'studyRef': {'refname': 'Example Project'},
            'samplesRef': [
                {'sampleAccession': 'SAMEA7851610', 'sampleId': '201903VIBRIO1185679118'},
                {'sampleAccession': 'SAMEA7851611', 'sampleId': '201903VIBRIO1185679119'},
                {'sampleAccession': 'SAMEA7851612', 'sampleId': '201903VIBRIO1185679120'},
                {'sampleAccession': 'SAMEA7851613', 'sampleId': '201903VIBRIO1185679121'},
                {'sampleAccession': 'SAMEA7851614', 'sampleId': '201903VIBRIO1185679122'}],
            'runRefs': [],
            'analysisType': {'sequenceVariation': {'assembly': {'custom': {'urlLink': 'http://abc.com'}}},
                             'experimentType': ['Genotyping by array'],
                             'software': 'software package GCTA, Burrows-Wheeler Alignment tool (BWA), HTSeq-python package',
                             'platform': 'BGISEQ-500',
                             'imputation': '1'},
            'files': [
                {'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz', 'fileType': 'vcf'},
                {'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi', 'fileType': 'tabix'}],
            'analysisLinks': [
                {'urlLink': {'label': 'abc', 'url': 'http://www.abc.com'}},
                {'urlLink': {'label': 'http://xyz.com', 'url': 'http://xyz.com'}},
                {'xrefLink': {'db': 'PubMed', 'id': '123456'}},
                {'xrefLink': {'db': 'PubMed', 'id': '789012', 'label': 'abcxyz'}}
            ]}

        ena_analysis_json_obj = self.converter._add_analysis(self.analysis, self.samples, self.files, self.project)
        self.assert_json_equal(expected_analysis_json_obj, ena_analysis_json_obj)

    # def test_add_analysis_to_existing_project(self):
    #     self.project_row['Project Alias'] = 'PRJEB00001'
    #
    #     root = ET.Element('ANALYSIS_SET')
    #     self.converter._add_analysis(root, self.analysis_row, self.project_row, self.sample_rows, self.file_rows)
    #     assert elements_equal(root, ET.fromstring(
    #         self.expected_analysis.replace('<STUDY_REF refname="TechFish"/>','<STUDY_REF accession="PRJEB00001"/>')
    #     ))

    def test_create_submission_json_obj(self):
        expected_submission_json_obj = {
            "alias": 'Submission-12345',
            'centre': 'University of Example',
            "actions": [
                {
                    "type": "ADD"
                },
                {
                    "type": "HOLD",
                    "holdUntilDate": "2025-01-04",
                }
            ]
        }

        with patch('eva_submission.ENA_submission.json_to_ENA_json.today',
                   return_value=datetime(year=2025, month=1, day=1)):
            ena_submission_json_obj = self.converter._create_ena_submission_json_obj(self.project,
                                                                                     'Submission-12345')
            self.assert_json_equal(expected_submission_json_obj, ena_submission_json_obj)

    def test_create_submission_json_obj_with_date(self):
        self.project['holdDate'] = "2023-06-25"
        expected_submission_json_obj = {
            "alias": 'Submission-12345',
            'centre': 'University of Example',
            "actions": [
                {
                    "type": "ADD"
                },
                {
                    "type": "HOLD",
                    "holdUntilDate": "2023-06-25",
                }
            ]
        }

        ena_submission_json_obj = self.converter._create_ena_submission_json_obj(self.project,
                                                                                 'Submission-12345')
        self.assert_json_equal(expected_submission_json_obj, ena_submission_json_obj)

    def test_create_ena_json_file(self):
        output_ena_json = self.converter.create_ena_submission()
        assert os.path.exists(output_ena_json)

    def test_create_submission_json_obj_for_existing_project(self):
        with patch.object(EnaJsonConverter, 'existing_project', new_callable=PropertyMock(return_value='PRJEB00001')):
            output_ena_json = self.converter.create_ena_submission()
            assert os.path.exists(output_ena_json)

            with open(output_ena_json, 'r') as file:
                ena_json_data = json.load(file)
                assert 'projects' in ena_json_data
                assert ena_json_data['projects'] == []
                assert 'analysis' in ena_json_data
                assert 'submission' in ena_json_data
                assert ena_json_data['submission']['alias'] == 'PRJEB00001_Submission-12345'

    def assert_json_equal(self, json1, json2):
        if json.dumps(json1, sort_keys=True) != json.dumps(json2, sort_keys=True):
            raise AssertionError(f"JSON objects are not equal.\nExpected: {json1}\nGot: {json2}")
