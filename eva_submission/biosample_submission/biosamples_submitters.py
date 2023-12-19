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

from copy import deepcopy
from datetime import datetime, date
from functools import lru_cache

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader

_now = datetime.now().isoformat()


class BioSamplesSubmitter(AppLogger):

    valid_actions = ('create', 'overwrite', 'curate', 'derive')

    def __init__(self, communicators, submit_type=('create',), allow_removal=False):
        assert len(communicators) > 0, 'Specify at least one communicator object to BioSamplesSubmitter'
        assert set(submit_type) <= set(self.valid_actions), f'all actions must be in {self.valid_actions}'
        self.default_communicator = communicators[0]
        self.communicators = communicators
        self.sample_name_to_accession = {}
        self.submit_type = submit_type
        self.allow_removal = allow_removal

    @lru_cache
    def _get_existing_sample(self, accession):
        return self.default_communicator.follows_link('samples', method='GET', join_url=accession)

    def can_create(self, sample):
        return 'create' in self.submit_type and 'accession' not in sample

    def can_overwrite(self, sample):
        """ We should overwrite a samples when it is owned by a domain supported current uploader"""
        return 'overwrite' in self.submit_type and \
            'accession' in sample and \
            self._get_communicator_for_sample(sample)

    def can_curate(self, sample):
        """ We can curate a samples if it has an existing accessionr"""
        return 'curate' in self.submit_type and 'accession' in sample

    def can_derive(self, sample):
        return 'derive' in self.submit_type and 'accession' in sample

    def _get_communicator_for_sample(self, sample):
        sample_data = self._get_existing_sample(sample.get('accession'))
        # This check If one of the account own the BioSample by checking if the 'domain' or 'webinSubmissionAccountId'
        # are the same as the one who submitted the sample
        for communicator in self.communicators:
            if communicator.communicator_attributes.items() <= sample_data.items():
                return communicator
        return None

    def validate_in_bsd(self, samples_data):
        for sample in samples_data:
            # If we're only retrieving don't need to validate.
            if self.can_create(sample) or self.can_overwrite(sample):
                sample.update(self.default_communicator.communicator_attributes)
                self.default_communicator.follows_link('samples', join_url='validate', method='POST', json=sample)

    def convert_sample_data_to_curation_object(self, future_sample):
        """Curation object can only change 3 attributes characteristics, externalReferences and relationships"""
        current_sample = self._get_existing_sample(future_sample.get('accession'))
        #FIXME: Remove this hack when this is fixed on BioSample's side
        # remove null values in externalReferences that causes crash when POSTing the curation object
        if 'externalReferences' in current_sample:
            current_sample['externalReferences'] = [
                dict([(k, v) for k, v in external_ref.items() if v is not None])
                for external_ref in current_sample['externalReferences']
            ]
        curation_object = {}
        attributes_pre = []
        attributes_post = []
        # To add or modify attributes
        attributes = set(future_sample['characteristics']).union(set(current_sample['characteristics']))
        for attribute in attributes:
            # Addition
            if attribute in future_sample['characteristics'] and attribute not in current_sample['characteristics']:
                post_attribute = future_sample['characteristics'].get(attribute)[0]
                attributes_post.append({
                    'type': attribute,
                    'value': post_attribute.get('text'),
                    **({'tag': post_attribute['tag']} if 'tag' in post_attribute else {})
                })
            # Removal
            elif self.allow_removal and \
                    attribute in current_sample['characteristics'] and \
                    attribute not in future_sample['characteristics']:
                pre_attribute = current_sample['characteristics'].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
            # Replacement
            elif attribute in future_sample['characteristics'] and attribute in current_sample['characteristics'] and \
                    future_sample['characteristics'][attribute] != current_sample['characteristics'][attribute]:
                pre_attribute = current_sample['characteristics'].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
                post_attribute = future_sample['characteristics'].get(attribute)[0]
                attributes_post.append({
                    'type': attribute,
                    'value': post_attribute.get('text'),
                    **({'tag': post_attribute['tag']} if 'tag' in post_attribute else {})
                })
        curation_object['attributesPre'] = attributes_pre
        curation_object['attributesPost'] = attributes_post
        # for externalReferences and relationships we're assuming you want to replace them all
        curation_object['externalReferencesPre'] = current_sample.get('externalReferences', [])
        curation_object['externalReferencesPost'] = future_sample.get('externalReferences', [])
        curation_object['relationshipsPre'] = current_sample.get('relationships', [])
        curation_object['relationshipsPost'] = future_sample.get('relationships', [])

        return dict(curation=curation_object, sample=future_sample.get('accession'))

    @staticmethod
    def _update_from_array(key, sample_source, sample_dest):
        """Add the element of an array stored in specified key from source to destination"""
        if key in sample_source:
            if key not in sample_dest:
                sample_dest[key] = []
            for element in sample_source[key]:
                if element not in sample_dest[key]:
                    sample_dest[key].append(element)

    def _update_samples_with(self, sample_source, sample_dest):
        """Update a BioSample object with the value of another"""
        for attribute in sample_source['characteristics']:
            if attribute not in sample_dest['characteristics']:
                sample_dest['characteristics'][attribute] = sample_source['characteristics'][attribute]
        self._update_from_array('externalReferences', sample_source, sample_dest)
        self._update_from_array('relationships', sample_source, sample_dest)
        self._update_from_array('contact', sample_source, sample_dest)
        self._update_from_array('organization', sample_source, sample_dest)
        for key in ['taxId', 'accession', 'name', 'release']:
            if key in sample_source and key not in sample_dest:
                sample_dest[key] = sample_source[key]

    def create_sample_to_overwrite(self, sample):
        """Create the sample that will be used to overwrite the exising ones"""
        # We only add existing characteristics if we do not want to remove anything
        if self.can_overwrite(sample) and not self.allow_removal:
            # retrieve the sample without any curation and add the new data on top
            current_sample = self._get_existing_sample(sample.get('accession'))
            self._update_samples_with(current_sample, sample)
        return sample

    def create_derived_sample(self, sample):
        skipped_attributes = ['SRA accession']
        if self.can_derive(sample):
            derived_sample = deepcopy(sample)
            current_sample = self._get_existing_sample(derived_sample.get('accession'))
            self._update_samples_with(current_sample, derived_sample)
            # Remove the accession of previous samples
            accession = derived_sample.pop('accession')
            # Remove the SRA accession if it is there
            if 'SRA accession' in derived_sample['characteristics']:
                derived_sample['characteristics'].pop('SRA accession')
            derived_sample['release'] = _now
            return derived_sample, accession

    def submit_biosamples_to_bsd(self, samples_data):
        """
        This function iterate through the multiple sample data to process and based on each sample characteristics and
        the submit_type will:
          - Create sample from provided annotation
          - Overwrite existing samples in BioSamples
          - Create curation objects and apply them to an existing sample
          - Derive a sample from an existing sample carrying over all its characteristics
        Then it returns a map of sample name to sample accession.
        """
        for sample in samples_data:
            sample.update(self.default_communicator.communicator_attributes)
            if self.can_create(sample):
                sample_json = self.default_communicator.follows_link('samples', method='POST', json=sample)
                self.debug('Accession sample ' + sample.get('name') + ' as ' + sample_json.get('accession'))
            elif self.can_overwrite(sample):
                sample_to_overwrite = self.create_sample_to_overwrite(sample)
                self.debug('Overwrite sample ' + sample.get('name') + ' with accession ' + sample.get('accession'))
                # Use the communicator that can own the sample to overwrite it.
                communicator = self._get_communicator_for_sample(sample)
                sample_json = communicator.follows_link('samples', method='PUT', join_url=sample.get('accession'),
                                                                     json=sample_to_overwrite)
            elif self.can_curate(sample):
                self.debug('Update sample ' + sample.get('name') + ' with accession ' + sample.get('accession'))
                curation_object = self.convert_sample_data_to_curation_object(sample)
                curation_json = self.default_communicator.follows_link(
                    'samples', method='POST', join_url=sample.get('accession')+'/curationlinks', json=curation_object
                )
                sample_json = sample
            elif self.can_derive(sample):
                derived_sample, original_accession = self.create_derived_sample(sample)
                sample_json = self.default_communicator.follows_link('samples', method='POST', json=derived_sample)
                if 'relationships' not in sample_json:
                    sample_json['relationships'] = []
                sample_json['relationships'].append(
                    {'type': "derived from", 'target': original_accession, 'source': sample_json['accession']}
                )
                sample_json = self.default_communicator.follows_link('samples', method='PUT',
                                                                     join_url=sample_json.get('accession'), json=sample_json)
                self.debug(f'Accession sample {sample.get("name")} as {sample_json.get("accession")} derived from'
                           f' {original_accession}')
            # Otherwise Keep the sample as is and retrieve the name so that list of sample to accession is complete
            else:
                sample_json = self._get_existing_sample(sample.get('accession'))
            self.sample_name_to_accession[sample_json.get('name')] = sample_json.get('accession')


class SampleSubmitter(AppLogger):

    sample_mapping = {}

    project_mapping = {}

    def __init__(self, submit_type):
        communicators = []
        # If the config has the credential for using webin with BioSamples use webin first
        if cfg.query('biosamples', 'webin_url') and cfg.query('biosamples', 'webin_username') and \
                cfg.query('biosamples', 'webin_password'):
            communicators.append(WebinHALCommunicator(
                cfg.query('biosamples', 'webin_url'), cfg.query('biosamples', 'bsd_url'),
                cfg.query('biosamples', 'webin_username'), cfg.query('biosamples', 'webin_password')
            ))
        communicators.append(AAPHALCommunicator(
            cfg.query('biosamples', 'aap_url'), cfg.query('biosamples', 'bsd_url'),
            cfg.query('biosamples', 'username'), cfg.query('biosamples', 'password'),
            cfg.query('biosamples', 'domain')
        ))
        self.submitter = BioSamplesSubmitter(communicators, submit_type)
        self.sample_data = None

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
        # Check that the data exists
        if self.sample_data:
            self.info('Validate {} sample(s) in BioSample'.format(len(self.sample_data)))
            self.submitter.validate_in_bsd(self.sample_data)
            self.info('Upload {} sample(s) '.format(len(self.sample_data)))
            self.submitter.submit_biosamples_to_bsd(self.sample_data)

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
                                'mating_type', 'sex', 'population', 'cell_type', 'dev_stage', 'germline', 'tissue_lib',
                                'tissue_type', 'culture_collection', 'specimen_voucher', 'collected_by',
                                'collection_date', 'geographic location (country and/or sea)',
                                'geographic location (region and locality)', 'host', 'identified_by',
                                'isolation_source', 'lat_lon', 'lab_host', 'environmental_sample', 'cultivar',
                                'ecotype', 'isolate', 'strain', 'sub_species', 'variety', 'sub_strain', 'cell_line',
                                'serotype', 'serovar']

    characteristic_defaults = {
        'collection_date': 'not provided',
        'geographic location (country and/or sea)': 'not provided'
    }

    submitter_mapping = {
        'Email Address': 'E-mail',
        'First Name': 'FirstName',
        'Last Name': 'LastName'
    }

    organisation_mapping = {
        'Laboratory': 'Name',
        'Address': 'Address',
    }

    def __init__(self, metadata_spreadsheet, submit_type=('create',)):
        super().__init__(submit_type=submit_type)
        self.metadata_spreadsheet = metadata_spreadsheet
        self.reader = EvaXlsxReader(self.metadata_spreadsheet)
        self.sample_data = self.map_metadata_to_bsd_data()

    @staticmethod
    def serialize(value):
        """Create a text representation of the value provided"""
        if isinstance(value, date):
            return value.strftime('%Y-%m-%d')
        return str(value)

    def map_metadata_to_bsd_data(self):
        payloads = []
        for sample_row in self.reader.samples:
            bsd_sample_entry = {'characteristics': {}}
            description_list = []
            if sample_row.get('Title'):
                description_list.append(sample_row.get('Title'))
            if sample_row.get('Description'):
                description_list.append(sample_row.get('Description'))
            if description_list:
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
                            [{'text': self.serialize(sample_row[key])}]
                        )
                    else:
                        # Ignore the other values
                        pass
            if sample_row.get('Novel attribute(s)'):
                for novel_attribute in sample_row.get('Novel attribute(s)').split(','):
                    if ":" in novel_attribute:
                        attribute, value = novel_attribute.strip().split(':')
                        self.apply_mapping(
                            bsd_sample_entry['characteristics'],
                            self.map_sample_key(attribute.lower()),
                            [{'text': self.serialize(value)}]
                        )
            # Apply defaults if the key doesn't already exist
            for key in self.characteristic_defaults:
                if key not in sample_row:
                    self.apply_mapping(
                        bsd_sample_entry['characteristics'],
                        self.map_sample_key(key.lower()),
                        [{'text': self.serialize(self.characteristic_defaults[key])}]
                    )
            project_row = self.reader.project
            for key in self.reader.project:
                if key in self.project_mapping:
                    self.apply_mapping(bsd_sample_entry['characteristics'], self.map_project_key(key),
                                       [{'text': self.serialize(project_row[key])}])
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
        return all((s.get("accession") for s in self.sample_data))

    def already_submitted_sample_names_to_accessions(self):
        if self.check_submit_done():
            return dict([
                (sample_row.get('Sample ID'), sample_row.get('Sample Accession')) for sample_row in self.reader.samples
            ])

    def all_sample_names(self):
        return [s.get('name') for s in self.sample_data]


class SampleReferenceSubmitter(SampleSubmitter):

    def __init__(self, biosample_accession_list, project_accession):
        super().__init__(submit_type=('curate',))
        self.biosample_accession_list = biosample_accession_list
        self.project_accession = project_accession
        self.sample_data = self.retrieve_biosamples()

    def retrieve_biosamples(self):
        biosample_objects = []
        eva_study_url = f'https://www.ebi.ac.uk/eva/?eva-study={self.project_accession}'
        for sample_accession in self.biosample_accession_list:
            sample_json = self.submitter._get_existing_sample(sample_accession)
            # remove any property that should not be uploaded again (the ones that starts with underscore)
            sample_json = dict([(prop, value) for prop, value in sample_json.items() if not prop.startswith('_')])
            # check if the external reference already exist before inserting it
            if not any([ref for ref in sample_json.get('externalReferences', []) if ref['url'] == eva_study_url]):
                if 'externalReferences' not in sample_json:
                    sample_json['externalReferences'] = []
                sample_json['externalReferences'].append({'url': eva_study_url})

            # FIXME: Remove this hack when this is fixed on BioSample's side
            # remove null values in externalReferences that causes crash when POSTing the curation object
            if 'externalReferences' in sample_json:
                sample_json['externalReferences'] = [
                    dict([(k, v) for k, v in external_ref.items() if v is not None])
                    for external_ref in sample_json['externalReferences']
                ]
            biosample_objects.append(sample_json)

        return biosample_objects
