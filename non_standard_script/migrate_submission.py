#!/usr/bin/env python

# Copyright 2022 EMBL - European Bioinformatics Institute
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
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing import sub_cli_utils
from eva_submission.submission_config import load_config
from eva_submission.submission_migration import SubmissionMigration

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Migrate an in-progress submission to the current cluster')
    argparse.add_argument('--submission_id', required=True, type=str, help='Submission ID of the submission to migrate')
    argparse.add_argument('--project', required=False, type=str, help='Optional associated project accession')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    submission = sub_cli_utils.fetch_submission(args.submission_id)
    if not submission:
        logger.error(f'Submission {args.submission_id} not found')
        sys.exit(1)

    with SubmissionMigration(args.submission_id) as submission_migration:
        submission_migration.migrate(args.project)


if __name__ == "__main__":
    main()
