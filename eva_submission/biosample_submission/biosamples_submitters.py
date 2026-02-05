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
from collections import defaultdict
from copy import deepcopy
from datetime import datetime, date
from functools import lru_cache

from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission.biosample_submission.biosample_converter_utils import update_sample_to_post_4_13
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader

_now = datetime.now().isoformat()

SAMPLE_IN_VCF_PROP = 'sampleInVCF'
BIOSAMPLE_ACCESSION_PROP = 'bioSampleAccession'
BIOSAMPLE_OBJECT_PROP = 'bioSampleObject'
CHARACTERISTICS_PROP = 'characteristics'
RELATIONSHIPS_PROP = 'relationships'
ACCESSION_PROP = 'accession'
SRA_ACCESSION_PROP = 'SRA accession'
RELEASE_PROP = 'release'
TAX_ID_PROP = 'taxId'
LAST_UPDATED_BY_PROP = 'last_updated_by'


def get_biosample_characteristics(sample, property_name, data_type='text'):
    prop = sample.get(BIOSAMPLE_OBJECT_PROP, {}).get(CHARACTERISTICS_PROP,{}).get(property_name)
    if prop:
        if len(prop) == 1:
            return prop[0].get(data_type)
        else:
            return [p.get(data_type) for p in prop]
    return None


class BioSamplesSubmitter(AppLogger):

    valid_actions = ('create', 'overwrite', 'override', 'curate', 'derive')
    characteristics_allowed_to_override = ('collection_date', 'geographic location (country and/or sea)',
                                           LAST_UPDATED_BY_PROP)

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
        return 'create' in self.submit_type and ACCESSION_PROP not in sample

    def can_overwrite(self, sample):
        """ We should overwrite a sample when it is owned by a domain supported by the current uploader
        or when we use a superuser to override the original sample"""
        return ACCESSION_PROP in sample and (
            'overwrite' in self.submit_type and self._get_communicator_for_sample(sample) or
            'override' in self.submit_type and self._allowed_to_override(sample)
        )

    def can_curate(self, sample):
        """ We can curate a samples if it has an existing accession"""
        return 'curate' in self.submit_type and ACCESSION_PROP in sample

    def can_derive(self, sample):
        return 'derive' in self.submit_type and ACCESSION_PROP in sample

    def _get_communicator_for_sample(self, sample):
        if 'override' in self.submit_type:
            return self.communicators[0]
        sample_data = self._get_existing_sample(sample.get(ACCESSION_PROP))
        # This check If one of the account own the BioSample by checking if the 'domain' or 'webinSubmissionAccountId'
        # are the same as the one who submitted the sample
        for communicator in self.communicators:
            if communicator.communicator_attributes.items() <= sample_data.items():
                return communicator
        return None

    def _allowed_to_override(self, sample):
        if sample.get(ACCESSION_PROP, '').startswith('SAMN'):
            return True
        else:
            self.warning(f'Sample {sample.get(ACCESSION_PROP)} cannot be overridden because it is not an NCBI sample ')

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
        current_sample = self._get_existing_sample(future_sample.get(ACCESSION_PROP), include_curation=True)
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
        attributes = set(future_sample[CHARACTERISTICS_PROP]).union(set(current_sample[CHARACTERISTICS_PROP]))
        for attribute in attributes:
            # Addition
            if attribute in future_sample[CHARACTERISTICS_PROP] and attribute not in current_sample[CHARACTERISTICS_PROP]:
                post_attribute = future_sample[CHARACTERISTICS_PROP].get(attribute)[0]
                attributes_post.append({
                    'type': attribute,
                    'value': post_attribute.get('text'),
                    **({'tag': post_attribute['tag']} if 'tag' in post_attribute else {})
                })
            # Removal
            elif self.allow_removal and \
                    attribute in current_sample[CHARACTERISTICS_PROP] and \
                    attribute not in future_sample[CHARACTERISTICS_PROP]:
                pre_attribute = current_sample[CHARACTERISTICS_PROP].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
            # Replacement
            elif attribute in future_sample[CHARACTERISTICS_PROP] and attribute in current_sample[CHARACTERISTICS_PROP] and \
                    future_sample[CHARACTERISTICS_PROP][attribute] != current_sample[CHARACTERISTICS_PROP][attribute]:
                pre_attribute = current_sample[CHARACTERISTICS_PROP].get(attribute)[0]
                attributes_pre.append({
                    'type': attribute,
                    'value': pre_attribute.get('text'),
                    **({'tag': pre_attribute['tag']} if 'tag' in pre_attribute else {})
                })
                post_attribute = future_sample[CHARACTERISTICS_PROP].get(attribute)[0]
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
        curation_object['relationshipsPre'] = current_sample.get(RELATIONSHIPS_PROP, [])
        curation_object['relationshipsPost'] = future_sample.get(RELATIONSHIPS_PROP, [])

        return dict(curation=curation_object, sample=future_sample.get(ACCESSION_PROP))

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
            tmp_sample_source = {CHARACTERISTICS_PROP: {}}
            for attribute in self.characteristics_allowed_to_override:
                if attribute in sample_source[CHARACTERISTICS_PROP]:
                    tmp_sample_source[CHARACTERISTICS_PROP][attribute] = sample_source[CHARACTERISTICS_PROP][attribute]
            sample_source = tmp_sample_source
        for attribute in sample_source[CHARACTERISTICS_PROP]:
            if attribute not in sample_dest[CHARACTERISTICS_PROP] or allow_overwrite:
                sample_dest[CHARACTERISTICS_PROP][attribute] = sample_source[CHARACTERISTICS_PROP][attribute]
        self._update_from_array('externalReferences', sample_source, sample_dest, allow_overwrite)
        self._update_from_array(RELATIONSHIPS_PROP, sample_source, sample_dest, allow_overwrite)
        self._update_from_array('contact', sample_source, sample_dest, allow_overwrite)
        self._update_from_array('organization', sample_source, sample_dest, allow_overwrite)
        for key in [TAX_ID_PROP, ACCESSION_PROP, 'name', RELEASE_PROP]:
            if key in sample_source and key not in sample_dest:
                sample_dest[key] = sample_source[key]

    def create_sample_to_overwrite(self, sample):
        """Create the sample that will be used to overwrite the exising ones"""
        # We only add existing characteristics if we do not want to remove anything
        destination_sample = None
        if self.can_overwrite(sample) and not self.allow_removal:
            # retrieve the sample without any curation and add the new data on top
            destination_sample = self._get_existing_sample(sample.get(ACCESSION_PROP))
            self._update_samples_with(sample, destination_sample, allow_overwrite=True)
        return destination_sample

    def create_derived_sample(self, sample):
        skipped_attributes = [SRA_ACCESSION_PROP]
        if self.can_derive(sample):
            derived_sample = deepcopy(sample)
            # There can be multiple source samples
            source_accessions = derived_sample.get(ACCESSION_PROP).split(',')
            for current_sample in [self._get_existing_sample(sa) for sa in source_accessions]:
                self._update_samples_with(current_sample, derived_sample)
                # Remove the accession of previous samples
                derived_sample.pop(ACCESSION_PROP)
                # Remove the SRA accession if it is there
                if SRA_ACCESSION_PROP in derived_sample[CHARACTERISTICS_PROP]:
                    derived_sample[CHARACTERISTICS_PROP].pop(SRA_ACCESSION_PROP)
            derived_sample[RELEASE_PROP] = _now
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
            self.debug('Accession sample ' + sample.get('name', '') + ' as ' + sample_json.get(ACCESSION_PROP))
        elif self.can_overwrite(sample):
            action_taken = 'overwrite'
            sample_to_overwrite = self.create_sample_to_overwrite(sample)
            self.debug('Overwrite sample ' + sample_to_overwrite.get('name', '') + ' with accession ' + sample_to_overwrite.get(ACCESSION_PROP))
            # Use the communicator that can own the sample to overwrite it.
            communicator = self._get_communicator_for_sample(sample)
            sample_json = communicator.follows_link('samples', method='PUT', join_url=sample.get(ACCESSION_PROP),
                                                    json=sample_to_overwrite)
        elif self.can_curate(sample):
            action_taken = 'curate'
            self.debug('Update sample ' + sample.get('name', '') + ' with accession ' + sample.get(ACCESSION_PROP))
            curation_object = self.convert_sample_data_to_curation_object(sample)
            curation_json = self.default_communicator.follows_link(
                'samples', method='POST', join_url=sample.get(ACCESSION_PROP) + '/curationlinks', json=curation_object
            )
            sample_json = sample
        elif self.can_derive(sample):
            action_taken = 'derive'
            sample.update(self.default_communicator.communicator_attributes)
            derived_sample, original_accessions = self.create_derived_sample(sample)
            sample_json = self.default_communicator.follows_link('samples', method='POST', json=derived_sample)
            if RELATIONSHIPS_PROP not in sample_json:
                sample_json[RELATIONSHIPS_PROP] = []
            for original_accession in original_accessions:
                sample_json[RELATIONSHIPS_PROP].append(
                    {'type': "derived from", 'target': original_accession, 'source': sample_json[ACCESSION_PROP]}
                )
            sample_json = self.default_communicator.follows_link('samples', method='PUT',
                                                                 join_url=sample_json.get(ACCESSION_PROP), json=sample_json)
            self.debug(f'Accession sample {sample.get("name")} as {sample_json.get(ACCESSION_PROP)} derived from'
                       f' {original_accessions}')
        # Otherwise Keep the sample as is and retrieve the name so that list of sample to accession is complete
        else:
            action_taken = 'None'
            sample_json = self._get_existing_sample(sample.get(ACCESSION_PROP))
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
            elif map_key.startswith(f'{CHARACTERISTICS_PROP}.'):
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
        action_to_sample_name = defaultdict(list)
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
                    sample_name_to_accession[sample_name] = sample_json.get(ACCESSION_PROP)
                    action_to_sample_name[action_taken].append(sample_name)
                else:
                    self.error(f'Sample {sample_name} is not a unique name. Sample {sample_accession} will not be stored')
                nb_sample_uploaded += 1
            elif sample_accession and sample_name_from_metadata:
                sample_name_to_accession[sample_name_from_metadata] = sample_accession
                action_to_sample_name['None'].append(sample_name_from_metadata)
        for action in action_to_sample_name:
            self.info(f'Action: {action} -> {len(action_to_sample_name[action])} sample(s)')
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

    characteristic_defaults = {
        'collection date': 'not provided',
        'geographic location (country and/or sea)': 'not provided'
    }

    def __init__(self, metadata_json, submit_type=('create',)):
        super().__init__(submit_type=submit_type)
        self.metadata_json = metadata_json

    def _convert_metadata(self):
        for sample in self.metadata_json.get('sample'):
            # If Biosample object is present then we are creating or updating a sample.
            # If not then just return the name and accession.
            if BIOSAMPLE_OBJECT_PROP not in sample:
                yield None, sample.get(SAMPLE_IN_VCF_PROP), sample.get(BIOSAMPLE_ACCESSION_PROP)
                continue
            # FIXME: handle BioSample JSON that uses old representation correctly
            if any(
                    old_attribute in sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP] and
                    new_attribute not in sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]
                    for old_attribute, new_attribute in [
                        ('geographicLocationCountrySea', 'geographic location (country and/or sea)'),
                        ('scientificName', 'scientific name'), ('collectionDate', 'collection date')
                ]):
                sample = update_sample_to_post_4_13(sample)
            bsd_sample_entry = {CHARACTERISTICS_PROP: {}}
            # TODO: Name should be set correctly by eva-sub-cli post v0.4.14. Remove this Hack when we don't want to support earlier version
            if 'name' not in sample[BIOSAMPLE_OBJECT_PROP]:
                sample_name = None
                if 'bioSampleName' in sample[BIOSAMPLE_OBJECT_PROP]:
                    sample_name = sample[BIOSAMPLE_OBJECT_PROP]['bioSampleName']
                    del sample[BIOSAMPLE_OBJECT_PROP]['bioSampleName']
                if 'bioSampleName' in sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]:
                    sample_name = sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]['bioSampleName'][0].get('text')
                    del sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]['bioSampleName']
                if sample_name:
                    sample[BIOSAMPLE_OBJECT_PROP]['name'] = sample_name
            # We are editing, curating, or deriving from a sample if the accession is present in the BioSample object
            if BIOSAMPLE_ACCESSION_PROP in sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]:
                bsd_sample_entry[ACCESSION_PROP] = sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP][BIOSAMPLE_ACCESSION_PROP][0]['text']
                del sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP][BIOSAMPLE_ACCESSION_PROP]
            bsd_sample_entry.update(sample[BIOSAMPLE_OBJECT_PROP])
            # Taxonomy ID should be present at top level as well
            if TAX_ID_PROP in sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP]:
                bsd_sample_entry[TAX_ID_PROP] = sample[BIOSAMPLE_OBJECT_PROP][CHARACTERISTICS_PROP][TAX_ID_PROP][0]['text']

            # Apply defaults if the key doesn't already exist
            for key in self.characteristic_defaults:
                if key not in bsd_sample_entry[CHARACTERISTICS_PROP]:
                    self.apply_mapping(
                        bsd_sample_entry[CHARACTERISTICS_PROP],
                        key,
                        [{'text': self.characteristic_defaults[key]}]
                        )
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
            bsd_sample_entry[RELEASE_PROP] = _now
            # Custom attributes added to all the BioSample we create/modify
            bsd_sample_entry[CHARACTERISTICS_PROP][LAST_UPDATED_BY_PROP] = [{'text': 'EVA'}]
            yield bsd_sample_entry, sample.get(SAMPLE_IN_VCF_PROP), sample.get(ACCESSION_PROP)

    def check_submit_done(self):
        return all([
            sample_json.get(BIOSAMPLE_ACCESSION_PROP)
            for sample_json in self.metadata_json.get('sample')
        ])

    def already_submitted_sample_names_to_accessions(self):
        """Provide a dict of name to BioSamples accession for pre-submitted samples."""
        return dict([
            (sample_json.get(SAMPLE_IN_VCF_PROP), sample_json.get(BIOSAMPLE_ACCESSION_PROP))
            for sample_json in self.metadata_json.get('sample')
            if BIOSAMPLE_ACCESSION_PROP in sample_json
        ])

    def all_sample_names(self):
        """This provides all the sample names regardless of their submission status"""
        return [sample_json.get(SAMPLE_IN_VCF_PROP) for sample_json in self.metadata_json.get('sample')]


class SampleMetadataSubmitter(SampleSubmitter):
    """Class that maps old version (before version 2) of the spreadsheet to Biosample json that can be submitted"""
    sample_mapping = {
        'Sample Name': 'name',
        'Sample Accession': ACCESSION_PROP,
        'Sex': f'{CHARACTERISTICS_PROP}.sex',
        'bio_material': f'{CHARACTERISTICS_PROP}.material',
        'Tax Id': TAX_ID_PROP,
        'Scientific Name': [f'{CHARACTERISTICS_PROP}.scientific name', f'{CHARACTERISTICS_PROP}.Organism'],
        'collection_date': f'{CHARACTERISTICS_PROP}.collection date'
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
            bsd_sample_entry = {CHARACTERISTICS_PROP: {}}
            description_list = []
            if sample_row.get('Title'):
                description_list.append(sample_row.get('Title'))
            if sample_row.get('Description'):
                description_list.append(sample_row.get('Description'))
            if description_list:
                self.apply_mapping(bsd_sample_entry[CHARACTERISTICS_PROP], 'description', [{'text': ' - '.join(description_list)}])
            for key in sample_row:
                if sample_row[key]:
                    if key in self.sample_mapping:
                        self.apply_mapping(bsd_sample_entry, self.map_sample_key(key), self.serialize(sample_row[key]))
                    elif key in self.accepted_characteristics:
                        # other field maps to characteristics
                        self.apply_mapping(
                            bsd_sample_entry[CHARACTERISTICS_PROP],
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
                            bsd_sample_entry[CHARACTERISTICS_PROP],
                            self.map_sample_key(attribute.lower()),
                            [{'text': self.serialize(value)}]
                        )
            # Apply defaults if the key doesn't already exist
            for key in self.characteristic_defaults:
                if key not in bsd_sample_entry[CHARACTERISTICS_PROP]:
                    self.apply_mapping(
                        bsd_sample_entry[CHARACTERISTICS_PROP],
                        self.map_sample_key(key.lower()),
                        [{'text': self.serialize(self.characteristic_defaults[key])}]
                    )
            project_row = self.reader.project
            for key in self.reader.project:
                if key in self.project_mapping:
                    self.apply_mapping(bsd_sample_entry[CHARACTERISTICS_PROP], self.map_project_key(key),
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

            bsd_sample_entry[RELEASE_PROP] = _now
            # Custom attributes added to all the BioSample we create/modify
            bsd_sample_entry[CHARACTERISTICS_PROP][LAST_UPDATED_BY_PROP] = [{'text': 'EVA'}]
            yield bsd_sample_entry, bsd_sample_entry.get('name'), bsd_sample_entry.get(ACCESSION_PROP)

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

