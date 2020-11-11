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
import os
import sys
from argparse import ArgumentParser

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from eva_submission.samples_checker import compare_spreadsheet_and_vcf
from eva_submission.submission_config import load_config
from eva_submission.eload_submission import Eload

logger = log_cfg.get_logger(__name__)



def main():
    argparse = ArgumentParser()
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    eload = Eload(args.eload)

    validation_config = {
        'metadata_file': eload.eload_cfg.query('submission', 'metadata_spreadsheet'),
        'vcf_files': eload.eload_cfg.query('submission', 'vcf_files'),
        'reference_fasta': eload.eload_cfg.query('submission', 'assembly_fasta'),
        'reference_report': eload.eload_cfg.query('submission', 'assembly_report'),
        'executable':  cfg['executable']
    }

    # Check if the files are in the xls if not add them
    compare_spreadsheet_and_vcf(eload.eload_cfg.query('submission', 'metadata_spreadsheet'), eload._get_dir('vcf'))

    # run the validation
    validation_confg_file = 'validation_confg_file.yaml'
    with open(validation_confg_file, 'w') as open_file:
        yaml.safe_dump(validation_config, open_file)
    validation_script = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nextflow', 'validation.nf')
    command_utils.run_command_with_output(
        'Start Nextflow Validation process',
        cfg['executable']['nextflow'] + ' ' + validation_script + ' -params-file ' + validation_confg_file
    )


if __name__ == "__main__":
    main()
