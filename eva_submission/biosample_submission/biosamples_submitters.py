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

from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission.biosample_submission.biosample_converter_utils import convert_sample
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader

_now = datetime.now().isoformat()

SAMPLE_IN_VCF = 'sampleInVCF'
BIOSAMPLE_ACCESSION = 'bioSampleAccession'
BIOSAMPLE_OBJECT = 'bioSampleObject'
CHARACTERISTICS = 'characteristics'
RELATIONSHIPS = 'relationships'
ACCESSION = 'accession'
SRA_ACCESSION = 'SRA accession'
RELEASE = 'release'
TAX_ID = 'taxId'
LAST_UPDATED_BY = 'last_updated_by'



class BioSamplesSubmitter(AppLogger):

    valid_actions = ('create', 'overwrite', 'override', 'curate', 'derive')
    characteristics_allowed_to_override = ('collection_date', 'geographic location (country and/or sea)',
                                           LAST_UPDATED_BY)

    def __init__(self, communicators, submit_type=('create',), allow_removal=False):
        assert len(communicators) > 0, 'Specify at least one communicator object to BioSamplesSubmitter'
        assert set(submit_type) <= set(self.valid_actions), f'all actions must be in {self.valid_actions}'
        self.default_communicator = communicators[0]
        self.communicators = communicators
        self.submit_type = submit_type
        self.allow_removal = allow_removal

    @lru_cache(maxsize=0)
    def _get_existing_sample(self, accession, include_curation=False):
        if include_curation:
            append_to_url = accession
        else:
            append_to_url = accession + '?curationdomain='
        return self.default_communicator.follows_link('samples', method='GET', join_url=append_to_url)

    def can_create(self, sample):
        return 'create' in self.submit_type and ACCESSION not in sample

    def can_overwrite(self, sample):
        """ We should overwrite a sample when it is owned by a domain supported by the current uploader
        or when we use a superuser to override the original sample"""
        return ACCESSION in sample and (
            'overwrite' in self.submit_type and self._get_communicator_for_sample(sample) or
            'override' in self.submit_type and self._allowed_to_override(sample)
        )

    def can_curate(self, sample):
        """ We can curate a samples if it has an existing accession"""
        return 'curate' in self.submit_type and ACCESSION in sample

    def can_derive(self, sample):
        return 'derive' in self.submit_type and ACCESSION in sample

    def _get_communicator_for_sample(self, sample):
        if 'override' in self.submit_type:
            return self.communicators[0]
        sample_data = self._get_existing_sample(sample.get(ACCESSION))
        # This check If one of the account own the BioSample by checking if the 'domain' or 'webinSubmissionAccountId'
        # are the same as the one who submitted the sample
        for communicator in self.communicators:
            if communicator.communicator_attributes.items() <= sample_data.items():
                return communicator
        return None

    def _allowed_to_override(self, sample):
        if sample.get(ACCESSION, '').startswith('SAMN'):
            return True
        else:
            self.warning(f'Sample {sample.get(ACCESSION)} cannot be overridden because it is not an NCBI sample ')

    def validate_in_bsd(self, sample_data):
        sample = deepcopy(sample_data)
        if self.can_overwrite(sample):
            sample = self.create_sample_to_overwrite(sample)

        # If we're only retrieving don't need to validate.
        if self.can_create(sample) or self.can_overwrite(sample):
            if self.can_create(sample):
                sample.update(self.default_communicator.communicator_attributes)
            self.default_communicator.follows_link('samples', join_url='validate', method='POST', json=sample)

    def convert_sample_data_to_curation_object(self, future_sample):
        """Curation object can only change 3 attributes characteristics, externalReferences and relationships"""
        current_sample = self._get_existing_sample(future_sample.get(ACCESSION), include_curation=True)
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
        attributes = set(future_sample[CHARACTERISTICS]).union(set(current_sample[CHARACTERISTICS]))
        for attribute in attributes:
            # Addition
            if attribute in future_sample[CHARACTERISTICS] and attribute not in current_sample[CHARACTERISTICS]:
                post_attribute = future_sample[CHARACTERISTICS].get(attribute)[0]
                attributes_post.append({
                    'type': attribute,
                    'value': post_attribute.get('text'),
                    **({'tag': post_attribute['tag']} if 'tag' in post_attribute else {})
                })
            # Removal
            elif self.allow_removal and \
                    attribute in current_sample[CHARACTERISTICS] and \
                    attribute not in future_sample[CHARACTERISTICS]:
                pre_attribute = current_sample[CHARACTERISTICS].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
            # Replacement
            elif attribute in future_sample[CHARACTERISTICS] and attribute in current_sample[CHARACTERISTICS] and \
                    future_sample[CHARACTERISTICS][attribute] != current_sample[CHARACTERISTICS][attribute]:
                pre_attribute = current_sample[CHARACTERISTICS].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
                post_attribute = future_sample[CHARACTERISTICS].get(attribute)[0]
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
        curation_object['relationshipsPre'] = current_sample.get(RELATIONSHIPS, [])
        curation_object['relationshipsPost'] = future_sample.get(RELATIONSHIPS, [])

        return dict(curation=curation_object, sample=future_sample.get(ACCESSION))

    @staticmethod
    def _update_from_array(key, sample_source, sample_dest, allow_overwrite=False):
        """Add the element of an array stored in specified key from source to destination"""
        if key in sample_source:
            if key not in sample_dest:
                sample_dest[key] = []
            for element in sample_source[key]:
                if element not in sample_dest[key] or allow_overwrite:
                    sample_dest[key].append(element)

    def _update_samples_with(self, sample_source, sample_dest, allow_overwrite=False):
        """Update a BioSample object with the value of another"""
        if 'override' in self.submit_type:
            # Ensure that override only change geographic location and collection date
            tmp_sample_source = {CHARACTERISTICS: {}}
            for attribute in self.characteristics_allowed_to_override:
                if attribute in sample_source[CHARACTERISTICS]:
                    tmp_sample_source[CHARACTERISTICS][attribute] = sample_source[CHARACTERISTICS][attribute]
            sample_source = tmp_sample_source
        for attribute in sample_source[CHARACTERISTICS]:
            if attribute not in sample_dest[CHARACTERISTICS] or allow_overwrite:
                sample_dest[CHARACTERISTICS][attribute] = sample_source[CHARACTERISTICS][attribute]
        self._update_from_array('externalReferences', sample_source, sample_dest, allow_overwrite)
        self._update_from_array(RELATIONSHIPS, sample_source, sample_dest, allow_overwrite)
        self._update_from_array('contact', sample_source, sample_dest, allow_overwrite)
        self._update_from_array('organization', sample_source, sample_dest, allow_overwrite)
        for key in [TAX_ID, ACCESSION, 'name', RELEASE]:
            if key in sample_source and key not in sample_dest:
                sample_dest[key] = sample_source[key]

    def create_sample_to_overwrite(self, sample):
        """Create the sample that will be used to overwrite the exising ones"""
        # We only add existing characteristics if we do not want to remove anything
        destination_sample = None
        if self.can_overwrite(sample) and not self.allow_removal:
            # retrieve the sample without any curation and add the new data on top
            destination_sample = self._get_existing_sample(sample.get(ACCESSION))
            self._update_samples_with(sample, destination_sample, allow_overwrite=True)
        return destination_sample

    def create_derived_sample(self, sample):
        skipped_attributes = [SRA_ACCESSION]
        if self.can_derive(sample):
            derived_sample = deepcopy(sample)
            # There can be multiple source samples
            source_accessions = derived_sample.get(ACCESSION).split(',')
            for current_sample in [self._get_existing_sample(sa) for sa in source_accessions]:
                self._update_samples_with(current_sample, derived_sample)
                # Remove the accession of previous samples
                derived_sample.pop(ACCESSION)
                # Remove the SRA accession if it is there
                if SRA_ACCESSION in derived_sample[CHARACTERISTICS]:
                    derived_sample[CHARACTERISTICS].pop(SRA_ACCESSION)
            derived_sample[RELEASE] = _now
            return derived_sample, source_accessions

    def submit_biosample_to_bsd(self, biosample_json):
        """
        Based on the submit_type, this function will:
          - Create sample from provided annotation
          - Overwrite existing samples in BioSamples
          - Create curation objects and apply them to an existing sample
          - Derive a sample from an existing sample carrying over all its characteristics
        Then it returns the resulting sample json that Biosample generate and the action taken.
        """
        sample = deepcopy(biosample_json)
        if self.can_create(biosample_json):
            action_taken = 'create'
            sample.update(self.default_communicator.communicator_attributes)
            sample_json = self.default_communicator.follows_link('samples', method='POST', json=sample)
            self.debug('Accession sample ' + sample.get('name', '') + ' as ' + sample_json.get(ACCESSION))
        elif self.can_overwrite(sample):
            action_taken = 'overwrite'
            sample_to_overwrite = self.create_sample_to_overwrite(sample)
            self.debug('Overwrite sample ' + sample_to_overwrite.get('name', '') + ' with accession ' + sample_to_overwrite.get(ACCESSION))
            # Use the communicator that can own the sample to overwrite it.
            communicator = self._get_communicator_for_sample(sample)
            sample_json = communicator.follows_link('samples', method='PUT', join_url=sample.get(ACCESSION),
                                                                 json=sample_to_overwrite)
        elif self.can_curate(sample):
            action_taken = 'curate'
            self.debug('Update sample ' + sample.get('name', '') + ' with accession ' + sample.get(ACCESSION))
            curation_object = self.convert_sample_data_to_curation_object(sample)
            curation_json = self.default_communicator.follows_link(
                'samples', method='POST', join_url=sample.get(ACCESSION)+'/curationlinks', json=curation_object
            )
            sample_json = sample
        elif self.can_derive(sample):
            action_taken = 'derive'
            sample.update(self.default_communicator.communicator_attributes)
            derived_sample, original_accessions = self.create_derived_sample(sample)
            sample_json = self.default_communicator.follows_link('samples', method='POST', json=derived_sample)
            if RELATIONSHIPS not in sample_json:
                sample_json[RELATIONSHIPS] = []
            for original_accession in original_accessions:
                sample_json[RELATIONSHIPS].append(
                    {'type': "derived from", 'target': original_accession, 'source': sample_json[ACCESSION]}
                )
            sample_json = self.default_communicator.follows_link('samples', method='PUT',
                                                                 join_url=sample_json.get(ACCESSION), json=sample_json)
            self.debug(f'Accession sample {sample.get("name")} as {sample_json.get(ACCESSION)} derived from'
                       f' {original_accessions}')
        # Otherwise Keep the sample as is and retrieve the name so that list of sample to accession is complete
        else:
            action_taken = 'None'
            sample_json = self._get_existing_sample(sample.get(ACCESSION))
        return sample_json, action_taken


class SampleSubmitter(AppLogger):

    sample_mapping = {}

    project_mapping = {}

    def __init__(self, submit_type):
        communicators = []
        # If the config has the credential for using webin with BioSamples use webin
        communicators.append(WebinHALCommunicator(
            cfg.query('biosamples', 'webin_url'), cfg.query('biosamples', 'bsd_url'),
            cfg.query('biosamples', 'webin_username'), cfg.query('biosamples', 'webin_password')
        ))
        self.submitter = BioSamplesSubmitter(communicators, submit_type)

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
            elif map_key.startswith(f'{CHARACTERISTICS}.'):
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

    def check_submit_done(self):
        raise NotImplementedError()

    def already_submitted_sample_names_to_accessions(self):
        raise NotImplementedError()

    def all_sample_names(self):
        raise NotImplementedError()

    def _convert_metadata(self):
        """
        Returns a tuple containing the biosample in json the unique sample name associated and the biosample accession.
        If the biosample is None then the accession  needs to be present
        If both the biosample and the accession are present then the Biosample will be overwritten
        """
        raise NotImplementedError()

    def submit_to_bioSamples(self):
        sample_name_to_accession = {}
        nb_sample_uploaded = 0
        for source_sample_json, sample_name_from_metadata, sample_accession in self._convert_metadata():
            if source_sample_json:
                self.submitter.validate_in_bsd(source_sample_json)
                sample_json, action_taken = self.submitter.submit_biosample_to_bsd(source_sample_json)
                # When a name is provided in the metadata, we use it to keep track of which samples have been accessioned.
                # When not provided, use the BioSample name
                if sample_name_from_metadata:
                    sample_name = sample_name_from_metadata
                else:
                    self.debug(f'Name from metadata is missing use {sample_json.get("name")} instead')
                    sample_name = sample_json.get('name')
                if sample_name not in sample_name_to_accession:
                    sample_name_to_accession[sample_name] = sample_json.get(ACCESSION)
                else:
                    self.error(f'Sample {sample_name} is not a unique name. Sample {sample_accession} will not be stored')
                nb_sample_uploaded += 1
            elif sample_accession:
                sample_name_to_accession[sample_name] = sample_accession
        self.info(f'Uploaded {nb_sample_uploaded} sample(s)')
        return sample_name_to_accession


class SampleJSONSubmitter(SampleSubmitter):
    """
    Class that maps the biosample submitted in JSON through eva-sub-cli to the JSON format that biosample accepts.
    The conversion should be minimals but this class tracks the association between the sample submitted and accession
    assigned.
    """

    submitter_mapping = {
        'email': 'E-mail',
        'firstName': 'FirstName',
        'lastName': 'LastName'
    }

    organisation_mapping = {
        'laboratory': 'Name',
        'address': 'Address',
    }

    def __init__(self, metadata_json, submit_type=('create',)):
        super().__init__(submit_type=submit_type)
        self.metadata_json = metadata_json

    def _convert_metadata(self):
        for sample in self.metadata_json.get('sample'):
            # Currently no ability to overwrite or curate existing samples via JSON, so we skip any existing samples
            if BIOSAMPLE_OBJECT not in sample:
                yield None, sample.get(SAMPLE_IN_VCF), sample.get(BIOSAMPLE_ACCESSION)
                continue
            # FIXME: handle BioSample JSON that uses old representation correctly
            if any(
                    old_attribute in sample[BIOSAMPLE_OBJECT][CHARACTERISTICS] and
                    new_attribute not in sample[BIOSAMPLE_OBJECT][CHARACTERISTICS]
                    for old_attribute, new_attribute in [
                        ('geographicLocationCountrySea', 'geographic location (country and/or sea)'),
                        ('scientificName', 'scientific name'), ('collectionDate', 'collection date')
                ]):
                sample = convert_sample(sample)

            bsd_sample_entry = {CHARACTERISTICS: {}}
            # TODO: Name should be set correctly by eva-sub-cli post v0.4.14. Remove this Hack when we don't want to support earlier version
            if 'name' not in sample[BIOSAMPLE_OBJECT]:
                sample_name = None
                if 'bioSampleName' in sample[BIOSAMPLE_OBJECT]:
                    sample_name = sample[BIOSAMPLE_ACCESSION]['bioSampleName']
                    del sample[BIOSAMPLE_OBJECT]['bioSampleName']
                if 'bioSampleName' in sample[BIOSAMPLE_OBJECT][CHARACTERISTICS]:
                    sample_name = sample[BIOSAMPLE_OBJECT][CHARACTERISTICS]['bioSampleName'][0].get('text')
                    del sample[BIOSAMPLE_OBJECT][CHARACTERISTICS]['bioSampleName']
                if sample_name:
                    sample[BIOSAMPLE_OBJECT]['name'] = sample_name
            bsd_sample_entry.update(sample[BIOSAMPLE_OBJECT])
            # Taxonomy ID should be present at top level as well
            if TAX_ID in sample[BIOSAMPLE_OBJECT][CHARACTERISTICS]:
                bsd_sample_entry[TAX_ID] = sample[BIOSAMPLE_OBJECT][CHARACTERISTICS][TAX_ID][0]['text']
            if 'submitterDetails' in self.metadata_json:
                # add the submitter information to each BioSample
                contacts = []
                organisations = []
                for submitter in self.metadata_json.get('submitterDetails'):
                    contact = {}
                    organisation = {}
                    for key in submitter:
                        self.apply_mapping(contact, self.submitter_mapping.get(key), submitter[key])
                        self.apply_mapping(organisation, self.organisation_mapping.get(key), submitter[key])
                    if contact:
                        contacts.append(contact)
                    if organisation:
                        organisations.append(organisation)
                self.apply_mapping(bsd_sample_entry, 'contact', contacts)
                self.apply_mapping(bsd_sample_entry, 'organization', organisations)
            bsd_sample_entry[RELEASE] = _now
            # Custom attributes added to all the BioSample we create/modify
            bsd_sample_entry[CHARACTERISTICS][LAST_UPDATED_BY] = [{'text': 'EVA'}]
            yield bsd_sample_entry, sample.get(SAMPLE_IN_VCF), sample.get(ACCESSION)

    def check_submit_done(self):
        return all([
            sample_json.get(BIOSAMPLE_ACCESSION)
            for sample_json in self.metadata_json.get('sample')
        ])

    def already_submitted_sample_names_to_accessions(self):
        """Provide a dict of name to BioSamples accession for pre-submitted samples."""
        return dict([
            (sample_json.get(SAMPLE_IN_VCF), sample_json.get(BIOSAMPLE_ACCESSION))
            for sample_json in self.metadata_json.get('sample')
            if BIOSAMPLE_ACCESSION in sample_json
        ])

    def all_sample_names(self):
        """This provides all the sample names regardless of their submission status"""
        return [sample_json.get(SAMPLE_IN_VCF) for sample_json in self.metadata_json.get('sample')]


class SampleMetadataSubmitter(SampleSubmitter):
    """Class that maps old version (before version 2) of the spreadsheet to Biosample json that can be submitted"""
    sample_mapping = {
        'Sample Name': 'name',
        'Sample Accession': ACCESSION,
        'Sex': f'{CHARACTERISTICS}.sex',
        'bio_material': f'{CHARACTERISTICS}.material',
        'Tax Id': TAX_ID,
        'Scientific Name': [f'{CHARACTERISTICS}.scientific name', f'{CHARACTERISTICS}.Organism'],
        'collection_date': f'{CHARACTERISTICS}.collection date'
    }
    accepted_characteristics = ['Unique Name Prefix', 'Subject', 'Derived From', 'Scientific Name', 'Common Name',
                                'mating_type', 'sex', 'population', 'cell_type', 'dev_stage', 'germline', 'tissue_lib',
                                'tissue_type', 'culture_collection', 'specimen_voucher', 'collected_by',
                                'collection date', 'geographic location (country and/or sea)',
                                'geographic location (region and locality)', 'host', 'identified_by',
                                'isolation_source', 'lat_lon', 'lab_host', 'environmental_sample', 'cultivar',
                                'ecotype', 'isolate', 'strain', 'sub_species', 'variety', 'sub_strain', 'cell_line',
                                'serotype', 'serovar']

    characteristic_defaults = {
        'collection date': 'not provided',
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

    @staticmethod
    def serialize(value):
        """Create a text representation of the value provided"""
        if isinstance(value, date):
            return value.strftime('%Y-%m-%d')
        return str(value)

    def _convert_metadata(self):
        for sample_row in self.reader.samples:
            bsd_sample_entry = {CHARACTERISTICS: {}}
            description_list = []
            if sample_row.get('Title'):
                description_list.append(sample_row.get('Title'))
            if sample_row.get('Description'):
                description_list.append(sample_row.get('Description'))
            if description_list:
                self.apply_mapping(bsd_sample_entry[CHARACTERISTICS], 'description', [{'text': ' - '.join(description_list)}])
            for key in sample_row:
                if sample_row[key]:
                    if key in self.sample_mapping:
                        self.apply_mapping(bsd_sample_entry, self.map_sample_key(key), self.serialize(sample_row[key]))
                    elif key in self.accepted_characteristics:
                        # other field maps to characteristics
                        self.apply_mapping(
                            bsd_sample_entry[CHARACTERISTICS],
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
                            bsd_sample_entry[CHARACTERISTICS],
                            self.map_sample_key(attribute.lower()),
                            [{'text': self.serialize(value)}]
                        )
            # Apply defaults if the key doesn't already exist
            for key in self.characteristic_defaults:
                if key not in bsd_sample_entry[CHARACTERISTICS]:
                    self.apply_mapping(
                        bsd_sample_entry[CHARACTERISTICS],
                        self.map_sample_key(key.lower()),
                        [{'text': self.serialize(self.characteristic_defaults[key])}]
                    )
            project_row = self.reader.project
            for key in self.reader.project:
                if key in self.project_mapping:
                    self.apply_mapping(bsd_sample_entry[CHARACTERISTICS], self.map_project_key(key),
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

            bsd_sample_entry[RELEASE] = _now
            # Custom attributes added to all the BioSample we create/modify
            bsd_sample_entry[CHARACTERISTICS][LAST_UPDATED_BY] = [{'text': 'EVA'}]
            yield bsd_sample_entry, bsd_sample_entry.get('name'), bsd_sample_entry.get(ACCESSION)

    def check_submit_done(self):
        return all(sample_row.get('Sample Accession') for sample_row in self.reader.samples)

    def already_submitted_sample_names_to_accessions(self):
        return dict([
            (sample_row.get('Sample ID'), sample_row.get('Sample Accession')) for sample_row in self.reader.samples
        ])

    def all_sample_names(self):
        # We need to get back to the reader to get all the names that were present in the spreadsheet
        return [sample_row.get('Sample Name') or sample_row.get('Sample ID') for sample_row in self.reader.samples]


class SampleReferenceSubmitter(SampleSubmitter):

    def __init__(self, biosample_accession_list, project_accession):
        super().__init__(submit_type=('curate',))
        self.biosample_accession_list = biosample_accession_list
        self.project_accession = project_accession

    def _convert_metadata(self):
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
            yield sample_json, sample_json.get('name'), sample_accession

