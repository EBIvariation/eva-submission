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

from eva_submission.biosamples_submission import AAPHALCommunicator, SampleMetadataSubmitter, BioSamplesSubmitter
from eva_submission.submission_config import load_config


def main():
    arg_parser = argparse.ArgumentParser(
        description='Update an existing BioSample using information present in a Spreadsheet. '
                    'The spreadsheet needs to contain a Sheet called Sample.')
    arg_parser.add_argument('--metadata_file', required=True,
                            help='Spreadsheet file containing the sample information')
    arg_parser.add_argument('--action', required=True, choices=('overwrite', 'curate', 'derive'),
                            help='Type of modification of the BioSamples that should be made. '
                                 '"overwrite" require that EVA owns the BioSample entity. '
                                 '"curate" will create curation object on top of the BioSample. These are not '
                                 'used by ENA. '
                                 '"derive" will create a new BioSample derived from the old one.')
    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()
    sample_submitter = SampleMetadataSubmitter(args.metadata_file, submit_type=(args.action,))
    sample_submitter.submit_to_bioSamples()


if __name__ == "__main__":
    main()
