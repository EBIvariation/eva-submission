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
import csv

from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


def main():
    arg_parser = argparse.ArgumentParser(
        description='Check ownership of existing Biosamples accessions from a metadata file')
    arg_parser.add_argument('--metadata_file', required=True,
                            help='Spreadsheet file containing the sample information. '
                                 'It should contains some pre-existing BioSample accession')
    arg_parser.add_argument('--output', required=True,
                            help='CSV file containing the ownership information for all existing samples in the '
                                 'metadata spreadsheet')
    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()
    metadata_reader = EvaXlsxReader(args.metadata_file)
    communicator = WebinHALCommunicator(
        cfg.query('biosamples', 'webin_url'), cfg.query('biosamples', 'bsd_url'),
        cfg.query('biosamples', 'webin_username'), cfg.query('biosamples', 'webin_password')
    )
    with open(args.output, 'w') as open_ouptut:
        sample_attrs = ['accession', 'name', 'domain', 'webinSubmissionAccountId', 'status']
        writer = csv.DictWriter(open_ouptut, fieldnames=sample_attrs + ['owner'])
        writer.writeheader()
        for sample_row in metadata_reader.samples:
            if sample_row.get('Sample Accession'):
                # Existing samples
                sample_accession = sample_row.get('Sample Accession').strip()
                res = {}
                try:
                    json_response = communicator.follows_link('samples', join_url=sample_accession)
                    if json_response:
                        for attr in sample_attrs:
                            res[attr] = json_response.get(attr)
                        if res['domain'] == 'subs.team-31' or res['webinSubmissionAccountId'] == 'Webin-1008':
                            res['owner'] = 'EVA'
                        elif res['domain'] == 'self.BiosampleImportNCBI':
                            res['owner'] = 'BioSamples'
                        else:
                            res['owner'] = 'Third party'
                except ValueError:
                    print(f'{sample_accession} does not exist or is private')
                    res = {'accession': sample_accession, 'name': '', 'domain': '', 'webinSubmissionAccountId': '',
                           'status': 'PRIVATE', 'owner': ''}
                writer.writerow(res)


if __name__ == "__main__":
    main()
