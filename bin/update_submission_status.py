#!/usr/bin/env python

# Copyright 2026 EMBL - European Bioinformatics Institute
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

from eva_sub_cli_processing.sub_cli_utils import (
    get_from_sub_ws, put_to_sub_ws, sub_ws_url_build, PROCESSING_STEPS, PROCESSING_STATUS
)
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def fetch_submission_from_eload(eload_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', eloadId=eload_id, size=1))
    content = response.get('content', [])
    if not content:
        logger.error(f'No submission found for ELOAD {eload_id}')
        sys.exit(1)
    return content[0]


def fetch_submission(submission_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', submissionId=submission_id, size=1))
    content = response.get('content', [])
    if not content:
        logger.error(f'Submission {submission_id} not found')
        sys.exit(1)
    return content[0]


def main():
    argparse = ArgumentParser(description='Update the processing status of a submission in the submission webservice')
    target = argparse.add_mutually_exclusive_group(required=True)
    target.add_argument('--submission_id', required=False, type=str,
                        help='Target submission by UUID')
    target.add_argument('--eload_id', required=False, type=int,
                        help='Target submission by ELOAD number (resolved to UUID via API)')
    argparse.add_argument('--step', required=True, choices=PROCESSING_STEPS,
                          help='Processing step to update')
    argparse.add_argument('--status', required=True, choices=PROCESSING_STATUS,
                          help='New processing status')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    if args.eload_id:
        submission = fetch_submission_from_eload(args.eload_id)
        submission_id = submission['submissionId']
        logger.info(f'Resolved ELOAD {args.eload_id} to submission {submission_id}')
    else:
        submission_id = args.submission_id
        submission = fetch_submission(submission_id)

    old_step = submission.get('processingStep') or 'None'
    old_status = submission.get('processingStatus') or 'None'

    put_to_sub_ws(sub_ws_url_build('admin', 'submission-process', submission_id, args.step, args.status))
    logger.info(f'Updated submission {submission_id}: {old_step}/{old_status} -> {args.step}/{args.status}')


if __name__ == '__main__':
    main()
