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
import os

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_submission.eload_utils import convert_spreadsheet_to_json
from eva_submission.submission_config import load_config


def main():
    arg_parser = argparse.ArgumentParser(
        description='Update an existing BioSample using information present in a JSON. '
                    'The json file needs to contain a section called sample.')
    arg_parser.add_argument('--metadata_file', required=True,
                            help='JSON file containing the sample information')
    arg_parser.add_argument('--action', required=True, choices=('overwrite', 'curate', 'derive', 'override'),
                            help='Type of modification of the BioSamples that should be made. '
                                 '"overwrite" and "override" will both change the original sample (precuration) with '
                                 'the modified sample defined in the JSON. overwrite will use EVA credentials '
                                 'where override will use superuser credentials. overwrite require that EVA owns the '
                                 'BioSample entity. override requires that the samples are from NCBI.'
                                  '"curate" will create curation object on top of the BioSample. These are not '
                                 'used by ENA. '
                                 '"derive" will create a new BioSample derived from the old one.')
    arg_parser.add_argument('--debug', action='store_true', default=False,
                            help='Set the script to output logging information at debug level')
    args = arg_parser.parse_args()

    if args.debug:
        log_cfg.add_stdout_handler(level=logging.DEBUG)

    # Load the config_file from default location
    load_config()
    if args.metadata_file.endswith('.xlsx'):
        metadata_json_file_path = os.path.basename(args.metadata_file).replace('.xlsx', '.json')
        convert_spreadsheet_to_json(args.metadata_file, metadata_json_file_path)
    else:
        metadata_json_file_path = args.metadata_file
    sample_submitter = SampleJSONSubmitter(metadata_json_file_path, submit_type=(args.action,))
    sample_name_to_accession = sample_submitter.submit_to_bioSamples()
    for sample_name, accession in sample_name_to_accession.items():
        print(f'{sample_name}: {accession}')


if __name__ == "__main__":
    main()
