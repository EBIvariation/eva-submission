#!/usr/bin/env python
# Copyright 2020 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import re
from csv import DictReader, DictWriter
from datetime import datetime

import requests
from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from retry import retry

from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader

_now = datetime.now().isoformat()


class HALNotReadyError(Exception):
    pass


class HALCommunicator(AppLogger):
    """
    This class helps navigate through REST API that uses the HAL standard.
    """
    acceptable_code = [200, 201]

    def __init__(self, aap_url, bsd_url, username, password):
        self.aap_url = aap_url
        self.bsd_url = bsd_url
        self.username = username
        self.password = password

    def _validate_response(self, response):
        """Check that the response has an acceptable code and raise if it does not"""
        if response.status_code not in self.acceptable_code:
            self.error(response.request.method + ': ' + response.request.url + " with " + str(response.request.body))
            self.error("headers: {}".format(response.request.headers))
            self.error("<{}>: {}".format(response.status_code, response.text))
            raise ValueError('The HTTP status code ({}) is not one of the acceptable codes ({})'.format(
                str(response.status_code), str(self.acceptable_code))
            )
        return response

    @cached_property
    def token(self):
        """Retrieve the token from the AAP REST API then cache it for further quering"""
        response = requests.get(self.aap_url, auth=(self.username, self.password))
        self._validate_response(response)
        return response.text

    @retry(exceptions=(ValueError, requests.RequestException), tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _req(self, method, url, **kwargs):
        """private method that sends a request using the specified method. It adds the headers required by bsd"""
        headers = {'Accept': 'application/hal+json', 'Authorization': 'Bearer ' + self.token}
        if 'json' in kwargs:
            headers['Content-Type'] = 'application/json'
        response = requests.request(
            method=method,
            url=url,
            headers=headers,
            **kwargs
        )
        self._validate_response(response)
        return response

    def follows(self, query, json_obj=None, method='GET', url_template_values=None, join_url=None, **kwargs):
        """
        Finds a link within the json_obj using a query string or list, modify the link using the
        url_template_values dictionary then query the link using the method and any additional keyword argument.
        If the json_obj is not specified then it will use the root query defined by the base url.
        """
        all_pages = kwargs.pop('all_pages', False)

        if json_obj is None:
            json_obj = self.root
        # Drill down into a dict using dot notation
        _json_obj = json_obj
        if isinstance(query, str):
            query_list = query.split('.')
        else:
            query_list = query
        for query_element in query_list:
            if query_element in _json_obj:
                _json_obj = _json_obj[query_element]
            else:
                raise KeyError('{} does not exist in json object'.format(query_element, _json_obj))
        if not isinstance(_json_obj, str):
            raise ValueError('The result of the query_string must be a string to use as a url')
        url = _json_obj
        # replace the template in the url with the value provided
        if url_template_values:
            for k, v in url_template_values.items():
                url = re.sub('{(' + k + ')(:.*)?}', v, url)
        if join_url:
            url += '/' + join_url
        # Now query the url
        json_response = self._req(method, url, **kwargs).json()

        # Depaginate the call if requested
        if all_pages is True:
            # This depagination code will iterate over all the pages available until the pages comes back  without a
            # next page. It stores the embedded elements in the initial query's json response
            content = json_response
            while 'next' in content.get('_links'):
                content = self._req(method, content.get('_links').get('next').get('href'), **kwargs).json()
                for key in content.get('_embedded'):
                    json_response['_embedded'][key].extend(content.get('_embedded').get(key))
            # Remove the pagination information as it is not relevant to the depaginated response
            if 'page' in json_response: json_response.pop('page')
            if 'first' in json_response['_links']: json_response['_links'].pop('first')
            if 'last' in json_response['_links']: json_response['_links'].pop('last')
            if 'next' in json_response['_links']: json_response['_links'].pop('next')
        return json_response

    def follows_link(self, key, json_obj=None, method='GET', url_template_values=None, join_url=None, **kwargs):
        """
        Same function as follows but construct the query_string from a single keyword surrounded by '_links' and 'href'.
        """
        return self.follows(('_links', key, 'href'),
                            json_obj=json_obj, method=method, url_template_values=url_template_values,
                            join_url=join_url, **kwargs)

    @cached_property
    def root(self):
        return self._req('GET', self.bsd_url).json()


class BSDSubmitter(AppLogger):

    def __init__(self, communicator, domain):
        self.communicator = communicator
        self.domain = domain
        self.sample_name_to_accession = {}

    def validate_in_bsd(self, samples_data):
        for sample in samples_data:
            sample['domain'] = self.domain
            self.communicator.follows_link('samples', join_url='validate', method='POST', json=sample)

    def submit_to_bsd(self,  samples_data):
        """
        This function creates or updates samples in BioSamples and return a map of sample name to sample accession
        """

        for sample in samples_data:
            sample['domain'] = self.domain
            if 'accession' not in sample:
                # Create a sample
                sample_json = self.communicator.follows_link('samples', method='POST', json=sample)
                self.debug('Accession sample ' + sample.get('name') + ' as ' + sample_json.get('accession'))
            else:
                # Update a sample
                self.debug('Update sample ' + sample.get('name') + ' with accession ' + sample.get('accession'))
                sample_json = self.communicator.follows_link('samples', method='PUT', join_url=sample.get('accession'),
                                                             json=sample)
            self.sample_name_to_accession[sample_json.get('name')] = sample_json.get('accession')


class SampleSubmitter(AppLogger):

    sample_mapping = {}

    project_mapping = {}

    def __init__(self):
        communicator = HALCommunicator(cfg.query('biosamples', 'aap_url'), cfg.query('biosamples', 'bsd_url'),
                                       cfg.query('biosamples', 'username'), cfg.query('biosamples', 'password'))
        self.submitter = BSDSubmitter(communicator, cfg.query('biosamples', 'domain'))

    @staticmethod
    def map_key(key, mapping):
        """
        Retrieve the mapping associated with the provided key or returns the key if it can't
        """
        if key in mapping:
            return mapping[key]
        else:
            return key

    def map_sample_key(self, key):
        return self.map_key(key, self.sample_mapping)

    def map_project_key(self, key):
        return self.map_key(key, self.project_mapping)

    @staticmethod
    def apply_mapping(bsd_data, map_key, value):
        """
        Set the value to the bsd_data dict in the specified key.
        If the key starts with characteristics then it puts it in the sub dictionary and apply the characteristics text
        format
        """
        if map_key and value:
            if isinstance(map_key, list):
                # If we are provided a list then apply to all elements of the list
                for element in map_key:
                    SampleSubmitter.apply_mapping(bsd_data, element, value)
            elif map_key.startswith('characteristics.'):
                keys = map_key.split('.')
                _bsd_data = bsd_data
                for k in keys[:-1]:
                    if k in _bsd_data:
                        _bsd_data = _bsd_data[k]
                    else:
                        raise KeyError('{} does not exist in dict {}'.format(k, _bsd_data))
                    _bsd_data[keys[-1]] = [{'text': value}]
            elif map_key:
                bsd_data[map_key] = value

    def _group_across_fields(self, grouped_data, header, values_to_group):
        """Populate the grouped_data with the values. The grouped_data variable will be changed by this function"""
        groupname = self.map_project_key(header.split()[0].lower())
        if groupname not in grouped_data:
            grouped_data[groupname] = []
            for i, value in enumerate(values_to_group.split('\t')):
                grouped_data[groupname].append({self.map_project_key(header): value})
        else:
            for i, value in enumerate(values_to_group.split('\t')):
                grouped_data[groupname][i][self.map_project_key(header)] = value

    def submit_to_bioSamples(self):
        raise NotImplementedError


class SampleTabSubmitter(SampleSubmitter):

    sample_mapping = {
        'Sample Name': 'name',
        'Sample Accession': 'accession',
        'Sample Description': 'characteristics.description',
        'Organism': 'characteristics.organism',
        'Sex': 'characteristics.sex',
        'Material': 'characteristics.material',
        'Term Source REF': None,
        'Term Source ID': 'taxId',  # This is a bit spurious: assumes that the "Term Source REF" is always NCBI Taxonomy
        'Scientific Name': 'scientific name',
        'Common Name': 'common name'
    }

    project_mapping = {
        'person': 'contact',
        'Organization Name': 'Name',
        'Organization Address': 'Address',
        'Person Email': 'E-mail',
        'Person First Name': 'FirstName',
        'Person Last Name': 'LastName'
    }

    def __init__(self, sampletab_file):
        super().__init__()
        self.sampletab_file = sampletab_file
        sampletab_base, ext = os.path.splitext(self.sampletab_file)
        self.accessioned_sampletab_file = sampletab_base + '_accessioned' + ext

    def map_sample_tab_to_bsd_data(self, sample_tab_data, project_tab):
        """
        Map each column provided in the sampletab file to a key in the API's sample schema.
        No validation is performed at this point.
        """
        payloads = []
        for sample_tab in sample_tab_data:
            bsd_sample_entry = {'characteristics': {}}
            for header in sample_tab:
                if header.startswith('Characteristic['):
                    # characteristic maps to characteristics
                    key = header[len('Characteristic['): -1]
                    self.apply_mapping(
                        bsd_sample_entry['characteristics'],
                        self.map_sample_key(key.lower()),
                        [{'text': sample_tab[header]}]
                    )
                else:
                    self.apply_mapping(bsd_sample_entry, self.map_sample_key(header), sample_tab[header])
            grouped_values = {}
            for header in project_tab:
                # Organisation and contact can contain multiple values that are split across several fields
                # this will group the across fields
                groupname = self.map_project_key(header.split()[0].lower())
                if groupname in ['organization', 'contact']:
                    self._group_across_fields(grouped_values, header, project_tab[header])
                else:
                    # All the other project level field are added to characteristics
                    self.apply_mapping(bsd_sample_entry['characteristics'], header.lower(), [{'text': project_tab[header]}])
            # Store the grouped values
            for groupname in grouped_values:
                self.apply_mapping(bsd_sample_entry, groupname, grouped_values[groupname])

            bsd_sample_entry['release'] = _now
            payloads.append(bsd_sample_entry)

        return payloads

    def parse_sample_tab(self):
        self.info('Parse ' + self.sampletab_file)
        self._parse_sample_tab(self.sampletab_file)

    @staticmethod
    def _parse_sample_tab(sampletab_file):
        msi_dict = {}
        scd_lines = []
        in_msi = in_scd = False
        with open(sampletab_file) as open_file:
            for line in open_file:
                if line.strip() == '[MSI]':
                    in_msi = True
                    in_scd = False
                elif line.strip() == '[SCD]':
                    in_msi = False
                    in_scd = True
                else:
                    if line.strip() == '':
                        continue
                    elif in_msi:
                        values = line.strip().split('\t')
                        msi_dict[values[0]] = '\t'.join(values[1:])
                    elif in_scd:
                        scd_lines.append(line.strip())
        reader = DictReader(scd_lines, dialect='excel-tab')
        return msi_dict, reader

    def write_sample_tab(self, samples_to_accessions):
        """Read the input again and add the accessions to the sample lines in the output"""
        with open(self.sampletab_file) as open_read, open(self.accessioned_sampletab_file, 'w') as open_write:
            scd_lines = []
            in_scd = False
            for line in open_read:
                if line.strip() == '[SCD]':
                    in_scd = True
                    open_write.write(line)
                elif in_scd:
                    scd_lines.append(line.strip())
                else:
                    open_write.write(line)

            reader = DictReader(scd_lines, dialect='excel-tab')
            fieldnames = ['Sample Accession'] + reader.fieldnames
            writer = DictWriter(open_write, fieldnames, dialect='excel-tab')
            writer.writeheader()
            for sample_dict in reader:
                sample_dict['Sample Accession'] = samples_to_accessions[sample_dict['Sample Name']]
                writer.writerow(sample_dict)

    def submit_to_bioSamples(self):
        msi_dict, scd_reader = self.parse_sample_tab()
        sample_data = self.map_sample_tab_to_bsd_data(scd_reader, msi_dict)
        self.info('Validate {} sample(s) '.format(len(sample_data)))
        self.submitter.validate_in_bsd(sample_data)

        # Only accessioned if it was not done before
        if os.path.exists(self.accessioned_sampletab_file):
            self.error('Accessioned file exist already ' + self.accessioned_sampletab_file)
            self.error('If you want to update the samples you should provide the accessioned file.')
            self.error('If you really want to submit the samples again delete or move the accessioned file.')
        elif sample_data:
            self.info('Upload {} sample(s) '.format(len(sample_data)))
            try:
                self.submitter.submit_to_bsd(sample_data)
            finally:
                if 'accession' not in sample_data[0]:
                    # No accession in the input so create an output file
                    self.write_sample_tab(self.submitter.sample_name_to_accession)
        else:
            self.error('No Sample found in the Sample tab file: ' + self.sampletab_file)

        return self.submitter.sample_name_to_accession


class SampleMetadataSubmitter(SampleSubmitter):

    sample_mapping = {
        'Sample Name': 'name',
        'Sample Accession': 'accession',
        'Sex': 'characteristics.sex',
        'bio_material': 'characteristics.material',
        'Tax Id': 'taxId',
        'Scientific Name': ['characteristics.scientific name', 'characteristics.Organism']
    }
    accepted_characteristics = ['Unique Name Prefix', 'Subject', 'Derived From', 'Scientific Name', 'Common Name',
                                'mating_type', 'sex', 'cell_type', 'dev_stage', 'germline', 'tissue_lib', 'tissue_type',
                                'culture_collection', 'specimen_voucher', 'collected_by', 'collection_date',
                                'geographic location (country and/or sea)', 'geographic location (region and locality)',
                                'host', 'identified_by', 'isolation_source', 'lat_lon', 'lab_host',
                                'environmental_sample', 'cultivar', 'ecotype', 'isolate', 'strain', 'sub_species',
                                'variety', 'sub_strain', 'cell_line', 'serotype', 'serovar']

    submitter_mapping = {
        'Email Address': 'E-mail',
        'First Name': 'FirstName',
        'Last Name': 'LastName'
    }

    organisation_mapping = {
        'Laboratory': 'Name',
        'Address': 'Address',
    }


    def __init__(self, metadata_spreadsheet):
        super().__init__()
        self.metadata_spreadsheet = metadata_spreadsheet
        self.reader = EvaXlsxReader(self.metadata_spreadsheet)
        self.sample_data = self.map_metadata_to_bsd_data()

    def map_metadata_to_bsd_data(self):
        payloads = []
        for sample_row in self.reader.samples:
            bsd_sample_entry = {'characteristics': {}}
            description_list = []
            if sample_row.get('Title'):
                description_list.append(sample_row.get('Title'))
            if sample_row.get('Description'):
                description_list.append(sample_row.get('Description'))
            self.apply_mapping(bsd_sample_entry['characteristics'], 'description', [{'text': ' - '.join(description_list)}])
            for key in sample_row:
                if sample_row[key]:
                    if key in self.sample_mapping:
                        self.apply_mapping(bsd_sample_entry, self.map_sample_key(key), sample_row[key])
                    elif key in self.accepted_characteristics:
                        # other field  maps to characteristics
                        self.apply_mapping(
                            bsd_sample_entry['characteristics'],
                            self.map_sample_key(key.lower()),
                            [{'text': sample_row[key]}]
                        )
                    else:
                        # Ignore the other values
                        # self.warning('Field %s in Sample was ignored', key)
                        pass
            if sample_row.get('Novel attribute(s)'):
                for novel_attribute in sample_row.get('Novel attribute(s)').split(','):
                    attribute, value = novel_attribute.split(':')
                    self.apply_mapping(
                        bsd_sample_entry['characteristics'],
                        self.map_sample_key(attribute.lower()),
                        [{'text': value}]
                    )

            project_row = self.reader.project
            for key in self.reader.project:
                if key in self.project_mapping:
                    self.apply_mapping(bsd_sample_entry['characteristics'], self.map_project_key(key),
                                       [{'text': project_row[key]}])
                else:
                    # Ignore the other values
                    pass
            contacts = []
            organisations = []
            for submitter_row in self.reader.submitters:
                contact = {}
                organisation = {}
                for key in submitter_row:
                    self.apply_mapping(contact, self.submitter_mapping.get(key), submitter_row[key])
                    self.apply_mapping(organisation, self.organisation_mapping.get(key), submitter_row[key])
                if contact:
                    contacts.append(contact)
                if organisation:
                    organisations.append(organisation)

            self.apply_mapping(bsd_sample_entry, 'contact', contacts)
            self.apply_mapping(bsd_sample_entry, 'organization', organisations)

            bsd_sample_entry['release'] = _now
            payloads.append(bsd_sample_entry)

        return payloads

    def check_submit_done(self):
        return all((s.get("Sample Accession") for s in self.sample_data))

    def submit_to_bioSamples(self):

        # Check that the data exists
        if self.sample_data:
            self.info('Validate {} sample(s) in BioSample'.format(len(self.sample_data)))
            self.submitter.validate_in_bsd(self.sample_data)
            self.info('Upload {} sample(s) '.format(len(self.sample_data)))
            self.submitter.submit_to_bsd(self.sample_data)

        return self.submitter.sample_name_to_accession