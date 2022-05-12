import copy
import os
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch, Mock, PropertyMock

import yaml

from eva_submission import biosamples_submission, ROOT_DIR
from eva_submission.biosamples_submission import HALCommunicator, BSDSubmitter, SampleMetadataSubmitter


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
        communicator = HALCommunicator(self.config.get('aap_url'), self.config.get('bsd_url'),
                                       self.config.get('username'), self.config.get('password'))
        self.submitter = BSDSubmitter(communicator, self.config.get('domain'))

    def test_validate_in_bsd(self):
        self.submitter.validate_in_bsd(self.sample_data)

    def test_submit_to_bsd_create(self):
        self.submitter.submit_to_bsd(self.sample_data)
        self.assertEqual(len(self.submitter.sample_name_to_accession), len(self.sample_data))
        self.assertEqual(list(self.submitter.sample_name_to_accession.keys()), ['LH1'])
        self.assertIsNotNone(self.submitter.sample_name_to_accession.get('LH1'))
        # Check that the sample actually exists
        accession = self.submitter.sample_name_to_accession.get('LH1')
        sample_json = self.submitter.communicator.follows_link('samples', join_url=accession)
        self.assertIsNotNone(sample_json)
        self.assertEqual(sample_json['name'], 'LH1')

    def test_submit_to_bsd_update(self):
        self.submitter.submit_to_bsd(self.sample_data)
        accession = self.submitter.sample_name_to_accession.get('LH1')
        sample_json = self.submitter.communicator.follows_link('samples', join_url=accession)
        self.assertEqual(sample_json['characteristics']['description'][0]['text'], 'yellow croaker sample 12')
        self.assertEqual(sample_json['characteristics']['collected_by'][0]['text'], 'first1 last1')

        # Modify the descriptions and remove the collected_by
        updated_data = copy.deepcopy(self.sample_data)
        modified_text = 'blue croaker sample 12'
        updated_data[0]['characteristics']['description'] = [{'text': modified_text}]
        updated_data[0]['characteristics'].pop('collected_by')
        updated_data[0]['characteristics']['new metadata'] = [{'text': 'New value'}]
        updated_data[0]['accession'] = accession
        self.submitter.submit_to_bsd(updated_data)
        accession = self.submitter.sample_name_to_accession.get('LH1')
        # Needs to avoid requests' cache otherwise we get the previous version
        updated_sample_json = self.submitter.communicator.follows_link('samples', join_url=accession,
                                                                       headers={'Cache-Control': 'no-cache'})
        self.assertEqual(updated_sample_json['characteristics']['new metadata'][0]['text'], 'New value')
        self.assertEqual(updated_sample_json['characteristics']['description'][0]['text'], 'blue croaker sample 12')
        self.assertNotIn('collected_by', updated_sample_json['characteristics'])

    def test_submit_to_bsd_no_change(self):
        self.submitter.submit_to_bsd(self.sample_data)
        accession = self.submitter.sample_name_to_accession.get('LH1')
        print(accession)
        # Make sure there are nothing in the sample_name_to_accession
        self.submitter.sample_name_to_accession.clear()
        with patch.object(HALCommunicator, 'follows_link', wraps=self.submitter.communicator.follows_link) as mock_fl:
            self.submitter.submit_to_bsd([{'accession': accession}])
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
                'scientific name': [{'text': 'Homo sapiens'}]
            }}
            for i in range(100)
        ]
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
