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
import logging

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.biosamples_submission import AAPHALCommunicator, SampleMetadataSubmitter, BioSamplesSubmitter, \
    WebinHALCommunicator
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxWriter, EvaXlsxReader

logger = log_cfg.get_logger(__name__)


class LastSampleCurationDeleter(BioSamplesSubmitter):

    def __init__(self, communicators):
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

    def delete_last_curation_of(self, accession):
        sample_data = self.default_communicator.follows_link('samples', method='GET', join_url=accession)
        curation_data = self.default_communicator.follows_link('curationLinks', json_obj=sample_data, all_pages=True)
        curation_links = curation_data.get('_embedded').get('curationLinks')
        curation_object = curation_links[-1]

        communicator = None
        # Check who owns the curation
        if curation_links[-1]['domain']:
            for c in self.communicators:
                if c.communicator_attributes.items() <= curation_object.items():
                    communicator = c
        if not communicator:
            self.warning(f'Curation object {curation_object["hash"]} is not owned by you')
        else:
            if curation_object:
                logger.info(
                    f'About to delete BioSamples curation {curation_object["hash"]} for accession {accession} which changed \n '
                    f'- attributes with {curation_object["curation"]["attributesPre"]} to {curation_object["curation"]["attributesPost"]}\n'
                    f'- External Ref with {curation_object["curation"]["externalReferencesPre"]} to {curation_object["curation"]["externalReferencesPost"]}\n'
                    f'- Relationship with {curation_object["curation"]["relationshipsPre"]} to {curation_object["curation"]["relationshipsPost"]}'
                )
                # self.default_communicator.follows_link(curation_object, 'self', method='DELETE')
                pass


def main():
    arg_parser = argparse.ArgumentParser(
        description='query Biosamples accessions from a file')
    arg_parser.add_argument('--accession_file', required=True,
                            help='file containing the list of accession to query')

    # Load the config_file from default location
    load_config()

    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()
    sample_submitter = SampleMetadataSubmitter(args.accession_file)



if __name__ == "__main__":
    main()
