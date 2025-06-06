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

import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing.sub_cli_to_eload_converter.sub_cli_to_eload_converter import SubCLIToEloadConverter
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Copies data from the ftp (if specified) and search for VCF and metadata files.'
                                          'then create a config file storing information about the eload')
    argparse.add_argument('--ftp_box', required=False, type=int, choices=range(1, 21),
                          help='box number where the data should have been uploaded. Required to copy the data from the FTP')
    argparse.add_argument('--submitter', required=False, type=str,
                          help='the name of the directory for that submitter. Required to copy the data from the FTP')
    argparse.add_argument('--submission_id', required=False, type=str, help='The Submission Id of the submission')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--taxid', required=False, type=str,
                          help='Override and replace the taxonomy id provided in the metadata spreadsheet.')
    argparse.add_argument('--reference', required=False, type=str,
                          help='Override and replace the reference sequence accession provided in the metadata '
                               'spreadsheet.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    if args.submission_id:
        with SubCLIToEloadConverter(args.eload, args.submission_id) as sub_cli_eload:
            sub_cli_eload.check_status()
            sub_cli_eload.retrieve_vcf_files_from_sub_cli_ftp_dir()
            sub_cli_eload.download_metadata_json_and_convert_to_xlsx()
            sub_cli_eload.detect_all(args.taxid, args.reference)
    else:
        with EloadPreparation(args.eload) as eload:
            if args.ftp_box and args.submitter:
                eload.copy_from_ftp(args.ftp_box, args.submitter)
            eload.detect_all(args.taxid, args.reference)


if __name__ == "__main__":
    main()
