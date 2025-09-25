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

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.biosample_submission.biosamples_submitters import SampleMetadataSubmitter, SampleJSONSubmitter
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxWriter, EvaXlsxReader


def main():
    arg_parser = argparse.ArgumentParser(
        description='Update an existing BioSample using information present in a Spreadsheet. '
                    'The spreadsheet needs to contain a Sheet called Sample.')
    arg_parser.add_argument('--metadata_file', required=True,
                            help='Spreadsheet file containing the sample information')
    arg_parser.add_argument('--action', required=True, choices=('overwrite', 'curate', 'derive', 'override'),
                            help='Type of modification of the BioSamples that should be made. '
                                 '"overwrite" and "override" will both change the original sample (precuration) with '
                                 'the modified sample defined in the spreadsheet. overwrite will use EVA credentials '
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
        sample_submitter = SampleMetadataSubmitter(args.metadata_file, submit_type=(args.action,))
    elif args.metadata_file.endswith('.json'):
        sample_submitter = SampleJSONSubmitter(args.metadata_file, submit_type=(args.action,))
    else:
        raise ValueError(f'Unsupported file type for {args.metadata_file}')
    sample_name_to_accession = sample_submitter.submit_to_bioSamples()
    if args.action == 'derive':
        # When deriving samples we need to copy the resulting accessions in the spreadsheet.
        eva_xls_reader = EvaXlsxReader(args.metadata_file)
        eva_xls_writer = EvaXlsxWriter(args.metadata_file)
        sample_rows = []
        for sample_row in eva_xls_reader.samples:
            if sample_row.get('Sample Name') in sample_name_to_accession:
                sample_row['Sample Accession'] = sample_name_to_accession[sample_row.get('Sample Name')]
                sample_row['Sample ID'] = sample_row.get('Sample ID')
            elif sample_row.get('Sample ID') in sample_name_to_accession:
                sample_row['Sample Accession'] = sample_name_to_accession[sample_row.get('Sample ID')]
            sample_rows.append(sample_row)
        eva_xls_writer.set_samples(sample_rows)
        eva_xls_writer.save()


if __name__ == "__main__":
    main()
