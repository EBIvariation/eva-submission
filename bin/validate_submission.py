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

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser()
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--validation_tasks', required=False, type=str, nargs='+',
                          default=EloadValidation.all_validation_tasks, choices=EloadValidation.all_validation_tasks,
                          help='task or set of tasks to perform during validation')
    argparse.add_argument('--set_as_valid', action='store_true', default=False,
                          help='Set the script to consider all validation tasks performed as valid in the final '
                               'evaluation. This does not affect the actual report but only change if the final '
                               'evaluation')
    argparse.add_argument('--report', action='store_true', default=False,
                      help='Set the script to only report the results base on previously run validation.')

    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    eload = EloadValidation(args.eload)
    if not args.report:

        eload.validate(args.validation_tasks, args.set_as_valid)
    eload.report()


if __name__ == "__main__":
    main()
