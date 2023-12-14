import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch, Mock, PropertyMock

import pytest
import yaml

from eva_submission import biosamples_submission, ROOT_DIR
from eva_submission.biosamples_submission import HALCommunicator, BioSamplesSubmitter, SampleMetadataSubmitter, \
    SampleReferenceSubmitter, AAPHALCommunicator, WebinHALCommunicator


class BSDTestCase(TestCase):

    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def tearDown(self):
        if os.path.exists(os.path.join(self.resources_folder, 'ELOAD_609biosamples_accessioned.txt')):
            os.remove(os.path.join(self.resources_folder, 'ELOAD_609biosamples_accessioned.txt'))


class TestHALCommunicator(BSDTestCase):

    @staticmethod
    def patch_token(token='token'):
        """Creates a patch for BSDCommunicator token attribute. it returns the token provided"""
        return patch.object(HALCommunicator, 'token', return_value=PropertyMock(return_value=token))

    def setUp(self) -> None:
        self.comm = HALCommunicator('http://aap.example.org', 'http://BSD.example.org', 'user', 'pass')

    def test_token(self):
        with patch('requests.get', return_value=Mock(text='token', status_code=200)) as mocked_get:
            self.assertEqual(self.comm.token, 'token')
            mocked_get.assert_called_once_with('http://aap.example.org', auth=('user', 'pass'))

    def test_req(self):
        with patch('requests.request', return_value=Mock(status_code=200)) as mocked_request, \
                patch.object(HALCommunicator, 'token', new_callable=PropertyMock(return_value='token')):
            self.comm._req('GET', 'http://BSD.example.org')
            mocked_request.assert_called_once_with(
                method='GET', url='http://BSD.example.org',
                headers={'Accept': 'application/hal+json', 'Authorization': 'Bearer token'}
            )

        with patch.object(HALCommunicator, 'token', new_callable=PropertyMock(return_value='token')), \
                patch('requests.request') as mocked_request:
            mocked_request.return_value = Mock(status_code=500, request=PropertyMock(url='text'))
            self.assertRaises(ValueError, self.comm._req, 'GET', 'http://BSD.example.org')

    def test_root(self):
        expected_json = {'json': 'values'}
        with patch.object(HALCommunicator, '_req') as mocked_req:
            mocked_req.return_value = Mock(json=Mock(return_value={'json': 'values'}))
            self.assertEqual(self.comm.root, expected_json)
            mocked_req.assert_called_once_with('GET', 'http://BSD.example.org')

    def test_follows(self):
        json_response = {'json': 'values'}
        # Patches the _req function that returns the Response object with a json function
        patch_req = patch.object(HALCommunicator, '_req', return_value=Mock(json=Mock(return_value=json_response)))

        # test follow url
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test', {'test': 'url'}), json_response)
            mocked_req.assert_any_call('GET', 'url')

        # test follow url with a template
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test', {'test': 'url/{id:*.}'}, url_template_values={'id': '1'}), json_response)
            mocked_req.assert_any_call('GET', 'url/1')

        # test follow url deep in the json_obj
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows('test1.test2', {'test1': {'test2': 'url'}}),  json_response)
            mocked_req.assert_any_call('GET', 'url')

        # test follow url wih specific verb and payload
        with patch_req as mocked_req:
            self.assertEqual(
                self.comm.follows('test', {'test': 'url'}, method='POST', json={'data': 'value'}),
                json_response
            )
            mocked_req.assert_any_call('POST', 'url', json={'data': 'value'})

        # test follow with depagination
        json_entries_with_next = {
            '_embedded': {'samples': [json_response, json_response]},
            '_links': {'next': {'href': 'url'}, 'first': {}, 'last': {}},
            'page': {}
        }
        json_entries_without_next = {
            '_embedded': {'samples': [json_response]},
            '_links': {},
            'page': {}
        }
        patch_req_with_pages = patch.object(HALCommunicator, '_req', side_effect=[
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_with_next))),
            Mock(json=Mock(return_value=deepcopy(json_entries_without_next))),
        ])
        # Without all_pages=True only returns the first page
        with patch_req_with_pages as mocked_req:
            observed_json = self.comm.follows('test', {'test': 'url'})
            self.assertEqual(observed_json, json_entries_with_next)
            self.assertEqual(len(observed_json['_embedded']['samples']), 2)
            mocked_req.assert_any_call('GET', 'url')

        # With all_pages=True returns the first page that contains all the embedded elements
        with patch_req_with_pages as mocked_req:
            observed_json = self.comm.follows('test', {'test': 'url'}, all_pages=True)
            self.assertEqual(len(observed_json['_embedded']['samples']), 7)
            self.assertEqual(mocked_req.call_count, 4)

    def test_follows_link(self):
        json_response = {'json': 'values'}
        # Patches the _req function that returns the Response object with a json function
        patch_req = patch.object(HALCommunicator, '_req', return_value=Mock(json=Mock(return_value=json_response)))

        # test basic follow
        with patch_req as mocked_req:
            self.assertEqual(self.comm.follows_link('test', {'_links': {'test': {'href': 'url'}}}), json_response)
            mocked_req.assert_any_call('GET', 'url')


class TestWebinHALCommunicator(BSDTestCase):
    def setUp(self) -> None:
        self.comm = WebinHALCommunicator('http://webin.example.org', 'http://BSD.example.org', 'user', 'pass')

    def test_communicator_attributes(self):
        assert self.comm.communicator_attributes == {'webinSubmissionAccountId': 'user'}

    def test_token(self):
        with patch('requests.post', return_value=Mock(text='token', status_code=200)) as mocked_post:
            self.assertEqual(self.comm.token, 'token')
            print(mocked_post.mock_calls)
            mocked_post.assert_called_once_with('http://webin.example.org',
                                                json={'authRealms': ['ENA'], 'password': 'pass', 'username': 'user'})


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

    # @pytest.mark.skip(reason='PUT function in dev server does not work')
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
        mock_fl.assert_called_once_with('samples', method='GET', join_url=accession)
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
        biosamples_submission._now = now
        expected_payload = [
            {'name': 'S%s' % (i + 1), 'taxId': 9606, 'release': now,
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
        biosamples_submission._now = now
        contacts = [
            {'LastName': 'John', 'FirstName': 'Doe', 'E-mail': 'john.doe@example.com'},
            {'LastName': 'Jane', 'FirstName': 'Doe', 'E-mail': 'jane.doe@example.com'}
        ]
        organizations = [{'Name': 'GPE', 'Address': 'The place to be'}, {'Name': 'GPE', 'Address': 'The place to be'}]
        updated_samples = [{
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
            'accession': 'SAMD1234' + str(567 + i),
            'contact': contacts, 'organization': organizations,
            'characteristics': {},
            'release': now
        } for i in range(10, 20)]
        new_samples = [{'name': 'S%s' % (i + 1), 'taxId': 9606, 'release': now,
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

