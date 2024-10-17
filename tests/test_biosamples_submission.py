import copy
import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch

import pytest
import yaml

from ebi_eva_common_pyutils.biosamples_communicators import AAPHALCommunicator, HALCommunicator, WebinHALCommunicator

from eva_submission import ROOT_DIR
from eva_submission.biosample_submission import biosamples_submitters
from eva_submission.biosample_submission.biosamples_submitters import BioSamplesSubmitter, SampleMetadataSubmitter, \
    SampleReferenceSubmitter


class BSDTestCase(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def tearDown(self):
        if os.path.exists(os.path.join(self.resources_folder, 'ELOAD_609biosamples_accessioned.txt')):
            os.remove(os.path.join(self.resources_folder, 'ELOAD_609biosamples_accessioned.txt'))


sample_data = [{
        'characteristics': {
            'description': [{'text': 'yellow croaker sample 12'}],
            'identified_by': [{'text': 'first1 last1'}],
            'collected_by': [{'text': 'first1 last1'}],
            'geographic location (country and/or sea)': [{'text': 'China'}],
            'organism': [{'text': 'Larimichthys polyactis'}],
            'scientific name': [{'text': 'Larimichthys polyactis'}],
            'geographic location (region and locality)': [{'text': 'East China Sea,Liuheng, Putuo, Zhejiang'}],
            'common name': [{'text': 'yellow croaker'}],
            'submission title': [{
                'text': 'Characterization of a large dataset of SNPs in Larimichthys polyactis using high throughput 2b-RAD sequencing'}],
            'submission identifier': [{'text': ''}],
            'submission description': [{
                'text': 'Single nucleotide polymorphism (SNP) characterization and genotyping of Larimichthys polyactis by using high-throughput 2b-RAD sequencing technology'}],
            'database name': [{'text': 'PRJNA592281'}],
            'term source name': [{'text': 'NCBI Taxonomy'}],
            'term source uri': [{'text': 'http://www.ncbi.nlm.nih.gov/taxonomy'}]
        },
        'name': 'LH1',
        'taxId': '334908',
        'contact': [
            {'LastName': 'first1', 'FirstName': 'last1', 'E-mail': 'test1@gmail.com'},
            {'LastName': 'first2', 'FirstName': 'last2', 'E-mail': 'test2@gmail.com'}
        ],
        'organization': [
            {'Name': 'Laboratory1', 'Address': 'Changzhi Island, Zhoushan, Zhejiang 316022, PR China'},
            {'Name': 'Laboratory1', 'Address': 'Changzhi Island, Zhoushan, Zhejiang 316022, PR China'}
        ],
        'release': '2020-07-06T19:09:29.090Z'}
    ]


class TestBSDSubmitter(BSDTestCase):
    """
    Integration tests that will contact a test server for BSD.
    """

    def setUp(self) -> None:
        file_name = os.path.join(self.resources_folder, 'bsd_submission.yaml')
        self.config = None
        if os.path.isfile(file_name):
            with open(file_name) as open_file:
                self.config = yaml.safe_load(open_file)
        self.sample_data = deepcopy(sample_data)
        self.communicator = AAPHALCommunicator(self.config.get('aap_url'), self.config.get('bsd_url'),
                                       self.config.get('username'), self.config.get('password'),
                                       self.config.get('domain'))
        self.submitter = BioSamplesSubmitter([self.communicator])

    def test_validate_in_bsd(self):
        self.submitter.validate_in_bsd(self.sample_data)

    def test_validate_partial_data(self):
        # Sample data without name should still validate
        self.submitter.validate_in_bsd([{'accession': 'SAME1234'}])

    def test_convert_sample_data_to_curation_object(self):

        sample_data = {
            'characteristics': {'organism': [{'text': 'Larimichthys polyactis'}]},
            'name': 'LH1',
            'release': '2020-07-06T19:09:29.090Z'
        }

        self.submitter.submit_biosamples_to_bsd([sample_data])
        expected_curation = {
            'curation': {'attributesPost': [{'type': 'description', 'value': 'yellow croaker sample 12'}],
                         'attributesPre': [],
                         'externalReferencesPost': [],
                         'externalReferencesPre': [],
                         'relationshipsPost': [],
                         'relationshipsPre': []},
            'sample': self.submitter.sample_name_to_accession.get('LH1')
        }
        sample_data_update = {'characteristics': {'description': [{'text': 'yellow croaker sample 12'}]},
                              'accession': self.submitter.sample_name_to_accession.get('LH1')}
        curation_object = self.submitter.convert_sample_data_to_curation_object(sample_data_update)
        self.assertEqual(curation_object, expected_curation)

    def test_submit_to_bsd_create(self):
        self.submitter.submit_biosamples_to_bsd(self.sample_data)
        self.assertEqual(len(self.submitter.sample_name_to_accession), len(self.sample_data))
        self.assertEqual(list(self.submitter.sample_name_to_accession.keys()), ['LH1'])
        self.assertIsNotNone(self.submitter.sample_name_to_accession.get('LH1'))
        # Check that the sample actually exists
        accession = self.submitter.sample_name_to_accession.get('LH1')
        sample_json = self.submitter.default_communicator.follows_link('samples', join_url=accession)
        self.assertIsNotNone(sample_json)
        self.assertEqual(sample_json['name'], 'LH1')

    def _test_submit_to_bsd_with_update(self, submitter_update, change_original, allow_removal):
        original_description = 'yellow croaker sample 12'
        updated_description = 'blue croaker sample 12'
        # Create an initial submission
        self.submitter.submit_biosamples_to_bsd(self.sample_data)
        accession = self.submitter.sample_name_to_accession.get('LH1')
        sample_json = self.submitter.default_communicator.follows_link('samples', join_url=accession)
        self.assertEqual(sample_json['characteristics']['description'][0]['text'], original_description)
        self.assertEqual(sample_json['characteristics']['collected_by'][0]['text'], 'first1 last1')

        # Modify the descriptions and remove the collected_by
        updated_sample = {
            'accession': accession,
            'name': 'LH1',
            'characteristics': {
                'description': [{'text': updated_description}],
                'new metadata': [{'text': 'New value'}],
                'organism': [{'text': 'Larimichthys polyactis'}]
            },
            'externalReferences': [{'url': 'https://www.ebi.ac.uk/eva/?eva-study=PRJEB001'}]
        }
        # Update the sample
        submitter_update.submit_biosamples_to_bsd([updated_sample])
        new_accession = submitter_update.sample_name_to_accession.get('LH1')
        # Needs to avoid requests' cache otherwise we get the previous version
        updated_sample_json = self.communicator.follows_link(
            'samples', join_url=new_accession, headers={'Cache-Control': 'no-cache'}
        )
        self.assertEqual(updated_sample_json['characteristics']['new metadata'][0]['text'], 'New value')
        self.assertEqual(updated_sample_json['characteristics']['description'][0]['text'], updated_description)
        if allow_removal:
            self.assertTrue('collected_by' not in updated_sample_json['characteristics'])
        else:
            self.assertEqual(sample_json['characteristics']['collected_by'][0]['text'], 'first1 last1')
        self.assertEqual(updated_sample_json['externalReferences'][0]['url'], 'https://www.ebi.ac.uk/eva/?eva-study=PRJEB001')
        # Get the original sample without curation
        original_json = self.communicator.follows_link(
            'samples', join_url=f'{accession}?curationdomain=', headers={'Cache-Control': 'no-cache'}
        )
        if change_original:
            self.assertEqual(original_json['characteristics']['description'][0]['text'], updated_description)
        else:
            # It has the original description
            self.assertEqual(original_json['characteristics']['description'][0]['text'], original_description)

    def test_submit_to_bsd_curate(self):
        submitter_curate = BioSamplesSubmitter([self.communicator], ('curate',))
        self._test_submit_to_bsd_with_update(submitter_curate, change_original=False, allow_removal=False)

    def test_submit_to_bsd_curate_with_removal(self):
        submitter_curate = BioSamplesSubmitter([self.communicator], ('curate',), allow_removal=True)
        self._test_submit_to_bsd_with_update(submitter_curate, change_original=False, allow_removal=True)

    @pytest.mark.skip(reason='PUT function in de        v server does not work')
    def test_submit_to_bsd_overwrite(self):
        submitter_overwrite = BioSamplesSubmitter([self.communicator], ('overwrite',))
        self._test_submit_to_bsd_with_update(submitter_overwrite, change_original=True, allow_removal=False)

    def test_submit_to_bsd_derive(self):
        submitter_derive = BioSamplesSubmitter([self.communicator], ('derive',))
        self._test_submit_to_bsd_with_update(submitter_derive, change_original=False, allow_removal=False)

    def test_submit_to_bsd_no_change(self):
        self.submitter.submit_biosamples_to_bsd(self.sample_data)
        accession = self.submitter.sample_name_to_accession.get('LH1')
        # Make sure there are nothing in the sample_name_to_accession
        self.submitter.sample_name_to_accession.clear()
        with patch.object(HALCommunicator, 'follows_link', wraps=self.submitter.default_communicator.follows_link) as mock_fl:
            self.submitter.submit_biosamples_to_bsd([{'accession': accession}])
        mock_fl.assert_called_once_with('samples', method='GET', join_url=accession + '?curationdomain=')
        self.assertEqual(self.submitter.sample_name_to_accession, {'LH1': accession})


class TestSampleMetadataSubmitter(BSDTestCase):

    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        metadata_file1 = os.path.join(brokering_folder, 'metadata_sheet.xlsx')
        metadata_file2 = os.path.join(brokering_folder, 'metadata_sheet2.xlsx')
        metadata_partial_file = os.path.join(brokering_folder, 'metadata_sheet_partial.xlsx')
        self.submitter_no_biosample_ids = SampleMetadataSubmitter(metadata_file1)
        self.submitter = SampleMetadataSubmitter(metadata_file2)
        self.submitter_partial_biosample_ids = SampleMetadataSubmitter(metadata_partial_file)

    def test_map_metadata_to_bsd_data(self):
        now = '2020-07-06T19:09:29.090Z'
        biosamples_submitters._now = now

        expected_payload = [
            {'name': 'S%s' % (i + 1), 'taxId': 9606, 'release': now,
             'last_updated_by': 'EVA',
             'contact': [{'LastName': 'John', 'FirstName': 'Doe', 'E-mail': 'john.doe@example.com'},
                         {'LastName': 'Jane', 'FirstName': 'Doe', 'E-mail': 'jane.doe@example.com'}],
             'organization': [{'Name': 'GPE', 'Address': 'The place to be'},
                              {'Name': 'GPE', 'Address': 'The place to be'}],
             'characteristics': {
                'Organism': [{'text': 'Homo sapiens'}],
                'description': [{'text': 'Sample %s' % (i+1)}],
                'scientific name': [{'text': 'Homo sapiens'}],
                'collection_date': [{'text': '2020-01-15'}],
                'geographic location (country and/or sea)': [{'text': 'not provided'}]
            }}
            for i in range(100)
        ]
        expected_payload[0]['characteristics'].update({
            'family': [{'text': '168'}],
            'label': [{'text': '576-168-1-1'}],
            'tree.ind': [{'text': '168-B1-R1'}]
        })
        payload = self.submitter.map_metadata_to_bsd_data()
        self.assertEqual(payload, expected_payload)

    def test_map_partial_metadata_to_bsd_data(self):
        now = '2020-07-06T19:09:29.090Z'
        biosamples_submitters._now = now
        contacts = [
            {'LastName': 'John', 'FirstName': 'Doe', 'E-mail': 'john.doe@example.com'},
            {'LastName': 'Jane', 'FirstName': 'Doe', 'E-mail': 'jane.doe@example.com'}
        ]
        organizations = [{'Name': 'GPE', 'Address': 'The place to be'}, {'Name': 'GPE', 'Address': 'The place to be'}]
        updated_samples = [{
            'last_updated_by': 'EVA',
            'accession': 'SAMD1234' + str(567 + i),
            'name': 'S%s' % (i + 1), 'taxId': 9606, 'release': now,
            'contact': contacts, 'organization': organizations,
            'characteristics': {
                'Organism': [{'text': 'Homo sapiens'}],
                'description': [{'text': 'Sample %s' % (i + 1)}],
                'scientific name': [{'text': 'Homo sapiens'}]
            }
        } for i in range(10)]
        existing_samples = [{
            'last_updated_by': 'EVA',
            'accession': 'SAMD1234' + str(567 + i),
            'contact': contacts, 'organization': organizations,
            'characteristics': {},
            'release': now
        } for i in range(10, 20)]
        new_samples = [{'last_updated_by': 'EVA', 'name': 'S%s' % (i + 1), 'taxId': 9606, 'release': now,
                        'contact': contacts, 'organization': organizations,
                        'characteristics': {
                            'Organism': [{'text': 'Homo sapiens'}],
                            'description': [{'text': 'Sample %s' % (i + 1)}],
                            'scientific name': [{'text': 'Homo sapiens'}]
                        }} for i in range(20, 100)]

        expected_payload = updated_samples + existing_samples + new_samples
        payload = self.submitter_partial_biosample_ids.map_metadata_to_bsd_data()
        assert expected_payload == payload

    def test_check_submit_done(self):
        # This data has already been brokered to BioSamples
        self.assertTrue(self.submitter_no_biosample_ids.check_submit_done())

    def test_check_submit_not_done(self):
        # This submitter contains data to broker to BioSamples
        self.assertFalse(self.submitter.check_submit_done())


class TestSampleReferenceSubmitter(BSDTestCase):

    def test_retrieve_biosamples(self):
        sample_accessions = ['SAME001', 'SAME002']
        project_accession = 'PRJEB001'
        sample_1 = {"name": "FakeSample1", "accession": "SAME001", "domain": "self.ExampleDomain", "_links": {}, 'externalReferences': [{'url': 'test_url', 'duo': None}]}
        sample_2 = {"name": "FakeSample2", "accession": "SAME002", "domain": "self.ExampleDomain", "_links": {}}
        with patch.object(HALCommunicator, 'follows_link', side_effect=[sample_1, sample_2]):
            self.submitter = SampleReferenceSubmitter(sample_accessions, project_accession)
        assert self.submitter.sample_data == [
            {'name': 'FakeSample1', 'accession': 'SAME001', 'domain': 'self.ExampleDomain', 'externalReferences': [{'url': 'test_url'}, {'url': 'https://www.ebi.ac.uk/eva/?eva-study=PRJEB001'}]},
            {'name': 'FakeSample2', 'accession': 'SAME002', 'domain': 'self.ExampleDomain', 'externalReferences': [{'url': 'https://www.ebi.ac.uk/eva/?eva-study=PRJEB001'}]}
        ]


class TestSampleMetadataOverwritter(BSDTestCase):
    samples = {
        # NCBI samples
        'SAMN1234567': {
            'accession': 'SAMN1234567',
            'name': 'Sample1',
            'characteristics': {
                'description': [{'text': 'Sample 1'}],
                'scientific name': [{'text': 'Larimichthys polyactis'}],
            },
            'release': '2020-07-06T19:09:29.090Z'},
        'SAMN1234568': {
            'accession': 'SAMN1234568',
            'name': 'Sample2',
            'characteristics': {
                'description': [{'text': 'Sample 2'}],
                'scientific name': [{'text': 'Larimichthys polyactis'}],
            },
            'release': '2020-07-06T19:09:29.090Z'},
        # EBI samples
        'SAME1234567': {
            'accession': 'SAMN1234567',
            'name': 'Sample1',
            'characteristics': {
                'description': [{'text': 'Sample 1'}],
                'scientific name': [{'text': 'Larimichthys polyactis'}],
            },
            'release': '2020-07-06T19:09:29.090Z'},
        'SAME1234568': {
            'accession': 'SAMN1234568',
            'name': 'Sample2',
            'characteristics': {
                'description': [{'text': 'Sample 2'}],
                'scientific name': [{'text': 'Larimichthys polyactis'}],
            },
            'release': '2020-07-06T19:09:29.090Z'}
    }

    @staticmethod
    def _get_fake_sample(accession, include_curation=False):
        return TestSampleMetadataOverwritter.samples.get(accession)

    def setUp(self) -> None:
        brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        self.metadata_file_ncbi = os.path.join(brokering_folder, 'metadata_sheet_ncbi.xlsx')
        self.metadata_file_ebi = os.path.join(brokering_folder, 'metadata_sheet_ebi.xlsx')

    def test_override_samples(self):
        with patch.object(BioSamplesSubmitter, '_get_existing_sample', side_effect=self._get_fake_sample), \
                patch.object(HALCommunicator, 'follows_link') as m_follows_link:
            sample1 = copy.copy(self.samples.get('SAMN1234567'))
            sample1['characteristics']['collection_date'] = [{'text': '1920-12-24'}]
            sample1['characteristics']['geographic location (country and/or sea)'] = [{'text': 'USA'}]
            sample2 = copy.copy(self.samples.get('SAMN1234568'))
            sample2['characteristics']['collection_date'] = [{'text': '1920-12-24'}]
            sample2['characteristics']['geographic location (country and/or sea)'] = [{'text': 'USA'}]

            sample_submitter = SampleMetadataSubmitter(self.metadata_file_ncbi, submit_type=('override',))
            sample_submitter.submit_to_bioSamples()

            m_follows_link.assert_any_call('samples', method='PUT', join_url='SAMN1234567', json=sample1)
            m_follows_link.assert_any_call('samples', method='PUT', join_url='SAMN1234568', json=sample2)

    def test_not_override_samples(self):
        with patch.object(BioSamplesSubmitter, '_get_existing_sample', side_effect=self._get_fake_sample), \
                patch.object(HALCommunicator, 'follows_link') as m_follows_link:
            sample_submitter = SampleMetadataSubmitter(self.metadata_file_ebi, submit_type=('override',))
            sample_submitter.submit_to_bioSamples()
        m_follows_link.assert_not_called()

