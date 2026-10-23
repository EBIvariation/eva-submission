#!/usr/bin/env python
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing.sub_cli_utils import fetch_submission
from eva_submission.submission_config import load_config
from eva_submission.submission_qc_checks import SubmissionQC

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

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Run QC checks on the submitted submission')
    argparse.add_argument('--submission_id', required=True, type=str, help='Submission ID of the submission')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()

    submission = fetch_submission(args.submission_id)
    if not submission:
        logger.error(f'Submission {args.submission_id} not found')
        sys.exit(1)

    with SubmissionQC(args.submission_id) as submission_qc:
        submission_qc.run_qc_checks_for_submission()


if __name__ == "__main__":
    main()
