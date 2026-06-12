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
SUBMISSION_SOURCES = ['email', 'eva-sub-cli']

# Maps CLI underscore arg names to API camelCase query parameter names
PARAM_MAP = {
    'submission_id':      'submissionId',
    'eload_id':           'eloadId',
    'uploaded_after':     'uploadedAfter',
    'submission_account': 'submissionAccount',
    'source':             'source',
    'processing_step':    'processingStep',
    'processing_status':  'processingStatus',
    'account_id':         'accountId',
    'uploaded_time':      'uploadedTime',
    'project_title':      'projectTitle'
}

def map_sort(sort_values):
    result = []
    for s in sort_values:
        parts = s.split(',', 1)
        field = PARAM_MAP.get(parts[0], default=parts[0])
        result.append(','.join([field] + parts[1:]))
    return result

def map_params(params):
    return  {PARAM_MAP[k]: v for k, v in vars(params).items() if k in PARAM_MAP and v is not None}


def fetch_submissions(**filters):
    page = 0
    all_submissions = []
    while True:
        url = sub_ws_url_build('admin', 'submissions', page=page, size=200, **filters)
        response = get_from_sub_ws(url)
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
    argparse.add_argument('--submission_id', required=False, type=str,
                          help='Filter by submission ID')
    argparse.add_argument('--eload_id', required=False, type=int,
                          help='Filter by eload ID')
    argparse.add_argument('--uploaded_after', required=False, type=str, metavar='DATE',
                          help='Filter submissions uploaded on or after this date (ISO-8601, e.g. 2024-01-15)')
    argparse.add_argument('--submission_account', required=False, type=str,
                          help='Filter by submission account')
    argparse.add_argument('--source', required=False, type=str, choices=SUBMISSION_SOURCES,
                          help='Filter by submission source')
    argparse.add_argument('--processing_step', required=False, choices=PROCESSING_STEPS,
                          help='Filter by processing step')
    argparse.add_argument('--processing_status', required=False, choices=PROCESSING_STATUS,
                          help='Filter by processing status')
    argparse.add_argument('--sort', required=False, nargs='*',  action='extend', metavar='FIELD[,asc|desc]',
                          help='Sort by field with optional direction (e.g. uploadedTime,desc); repeatable')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    filter_params = {PARAM_MAP[k]: v for k, v in vars(args).items() if k in PARAM_MAP and v is not None}
    sort = map_sort(args.sort) if args.sort else None
    submissions = fetch_submissions(sort=sort, **filter_params)

    rows = [tuple(str(s.get(field) or '') for field in DISPLAY_FIELDS) for s in submissions]
    pretty_print(DISPLAY_FIELDS, rows)
    print(f'Total: {len(submissions)} submission(s)')


if __name__ == '__main__':
    main()
