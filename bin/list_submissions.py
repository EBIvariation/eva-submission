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
from argparse import ArgumentParser

from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing.sub_cli_utils import (
    get_from_sub_ws, sub_ws_url_build, PROCESSING_STEPS, PROCESSING_STATUS
)
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)

DISPLAY_FIELDS = ['submissionId', 'accountId', 'eloadId', 'uploadedTime', 'processingStep', 'processingStatus',
                  'projectTitle']


def fetch_submissions(**filters):
    page = 0
    all_submissions = []
    while True:
        response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', page=page, size=200, **filters))
        content = response.get('content', [])
        if not content:
            break
        all_submissions.extend(content)
        total_pages = response.get('totalPages', 1)
        if page >= total_pages - 1:
            break
        page += 1
    return all_submissions


def main():
    argparse = ArgumentParser(description='List submissions from the submission webservice')
    argparse.add_argument('--submission_id', required=False, type=str, dest='submissionId',
                          help='Filter by submission ID')
    argparse.add_argument('--eload_id', required=False, type=int, dest='eloadId',
                          help='Filter by eload ID')
    argparse.add_argument('--uploaded_after', required=False, type=str, metavar='DATE', dest='uploadedAfter',
                          help='Filter submissions uploaded on or after this date (ISO-8601, e.g. 2024-01-15)')
    argparse.add_argument('--submission_account', required=False, type=str, dest='submissionAccount',
                          help='Filter by submission account')
    argparse.add_argument('--source', required=False, type=str,
                          help='Filter by submission source (e.g. email, sub_cli)')
    argparse.add_argument('--processing_step', required=False, choices=PROCESSING_STEPS, dest='processingStep',
                          help='Filter by processing step')
    argparse.add_argument('--processing_status', required=False, choices=PROCESSING_STATUS, dest='processingStatus',
                          help='Filter by processing status')
    argparse.add_argument('--sort', required=False, type=str, metavar='FIELD[,asc|desc]',
                          help='Sort by field, optionally with direction (e.g. uploadedTime,desc)')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    filter_params = {k: v for k, v in vars(args).items() if k != 'debug' and v is not None}
    submissions = fetch_submissions(**filter_params)

    rows = [tuple(str(s.get(field) or '') for field in DISPLAY_FIELDS) for s in submissions]
    pretty_print(DISPLAY_FIELDS, rows)
    print(f'Total: {len(submissions)} submission(s)')


if __name__ == '__main__':
    main()
