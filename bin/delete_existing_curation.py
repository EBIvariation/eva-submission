#!/usr/bin/env python

# Copyright 2023 EMBL - European Bioinformatics Institute
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

import argparse

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.biosample_submission.biosamples_communicators import WebinHALCommunicator, AAPHALCommunicator
from eva_submission.biosample_submission.biosamples_submitters import BioSamplesSubmitter
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


class LastSampleCurationDeleter(BioSamplesSubmitter):

    def __init__(self, communicators=None):
        if not communicators:
            communicators = LastSampleCurationDeleter.get_config_communicators()
        super().__init__(communicators, submit_type=('curate',), allow_removal=True)


    @staticmethod
    def get_config_communicators():
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
        return communicators

    def delete_last_curation_of(self, accession, delete=False):
        sample_data = self.default_communicator.follows_link('samples', method='GET', join_url=accession)
        curation_data = self.default_communicator.follows_link('curationLinks', json_obj=sample_data, all_pages=True)
        curation_links = curation_data.get('_embedded').get('curationLinks')
        curation_object = curation_links[-1]

        communicator = None
        # Check if we own the curation
        for c in self.communicators:
            if c.communicator_attributes.items() <= curation_object.items():
                communicator = c
        if not communicator:
            self.warning(f'Curation object {curation_object["hash"]} is not owned by you: '
                         f'Webin: {curation_object["webinSubmissionAccountId"]} - '
                         f'Domain: {curation_object["domain"]}')
        else:
            if curation_object:
                logger.info(
                    f'About to delete BioSamples curation {curation_object["hash"]} for accession {accession} which changed\n '
                    f'- attributes with {curation_object["curation"]["attributesPre"]} to {curation_object["curation"]["attributesPost"]}\n'
                    f'- External Ref with {curation_object["curation"]["externalReferencesPre"]} to {curation_object["curation"]["externalReferencesPost"]}\n'
                    f'- Relationship with {curation_object["curation"]["relationshipsPre"]} to {curation_object["curation"]["relationshipsPost"]}'
                )
                if delete:
                    self.default_communicator.follows_link('self', json_obj=curation_object, method='DELETE')
                pass


def main():
    arg_parser = argparse.ArgumentParser(
        description='query Biosamples accessions from a file')
    arg_parser.add_argument('--accession_file', required=True,
                            help='file containing the list of accession to query')
    arg_parser.add_argument('--set_delete', action='store_true', default=False, help='Actually does the deletion')
    # Load the config_file from default location
    load_config()

    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()
    last_sample_curation_deleter = LastSampleCurationDeleter()
    with open(args.accession_file) as open_file:
        for line in open_file:
            accession = line.strip()
            last_sample_curation_deleter.delete_last_curation_of(accession, args.set_delete)


if __name__ == "__main__":
    main()