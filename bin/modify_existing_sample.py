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
import json
import logging
import os

import eva_sub_cli
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.spreadsheet.metadata_xlsx_utils import metadata_xlsx_version
from eva_sub_cli.executables.xlsx2json import XlsxParser, WORKSHEETS_KEY_NAME, SAMPLE, OPTIONAL_HEADERS_KEY_NAME, \
    SAMPLE_ACCESSION_KEY, REQUIRED_HEADERS_KEY_NAME, ANALYSIS_ALIAS_KEY, SAMPLE_NAME_IN_VCF_KEY
from packaging.version import Version

from eva_submission.biosample_submission.biosamples_submitters import SampleJSONSubmitter
from eva_submission.eload_utils import convert_spreadsheet_to_json
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)

class XlsxExistingSampleParser(XlsxParser):
    # This function will create a biosample json that contains both the biosample accession and the biosample object
    def get_sample_json_data(self):
        json_key = self.xlsx_conf[WORKSHEETS_KEY_NAME][SAMPLE]
        sample_json = {json_key: []}
        for row in self.get_rows():

            row_num = row.pop('row_num')
            json_value = {self.translate_header(SAMPLE, k): v for k, v in row.items() if v is not None}
            bio_sample_acc = self.xlsx_conf[SAMPLE][OPTIONAL_HEADERS_KEY_NAME][SAMPLE_ACCESSION_KEY]

            analysis_alias = self.xlsx_conf[SAMPLE][REQUIRED_HEADERS_KEY_NAME][ANALYSIS_ALIAS_KEY]
            sample_name_in_vcf = self.xlsx_conf[SAMPLE][REQUIRED_HEADERS_KEY_NAME][SAMPLE_NAME_IN_VCF_KEY]
            sample_data = self.get_sample_data_with_split_analysis_alias(json_value, analysis_alias, sample_name_in_vcf)

            if bio_sample_acc in json_value and json_value[bio_sample_acc]:
                sample_data.update(bioSampleAccession=json_value[bio_sample_acc])
            json_value.pop(analysis_alias)
            json_value.pop(sample_name_in_vcf)
            biosample_obj = self.get_biosample_object(json_value)
            sample_data.update(bioSampleObject=biosample_obj)
            sample_json[json_key].append(sample_data)

        return sample_json


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
        convert_spreadsheet_to_json(args.metadata_file, metadata_json_file_path, xls_parser=XlsxExistingSampleParser)

    else:
        metadata_json_file_path = args.metadata_file
    with open(metadata_json_file_path, 'r') as f:
        metadata_json = json.load(f)
        sample_submitter = SampleJSONSubmitter(metadata_json, submit_type=(args.action,))
        sample_name_to_accession = sample_submitter.submit_to_bioSamples()
        for sample_name, accession in sample_name_to_accession.items():
            print(f'{sample_name}: {accession}')


if __name__ == "__main__":
    main()
