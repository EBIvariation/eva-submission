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

from eva_submission.biosamples_submission import AAPHALCommunicator
from eva_submission.submission_config import load_config


def main():
    arg_parser = argparse.ArgumentParser(
        description='query Biosamples accessions from a file')
    arg_parser.add_argument('--accession_file', required=True,
                            help='file containing the list of accession to query')
    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()
    communicator = AAPHALCommunicator(cfg.query('biosamples', 'aap_url'), cfg.query('biosamples', 'bsd_url'),
                                      cfg.query('biosamples', 'username'), cfg.query('biosamples', 'password'),
                                      cfg.query('biosamples', 'domain'))
    with open(args.accession_file) as open_file:
        for sample_accession in open_file:
            sample_accession = sample_accession.strip()
            try:
                response = communicator.follows_link('samples', join_url=sample_accession)
            except ValueError:
                print(f'{sample_accession} does not exist or is private')
                continue
            if response:
                print(f'{sample_accession} exist and is public')


if __name__ == "__main__":
    main()
