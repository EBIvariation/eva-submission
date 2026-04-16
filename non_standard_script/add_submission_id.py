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

import json
import logging
import os
from argparse import ArgumentParser

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_sub_cli_processing.sub_cli_to_eload_converter.sub_cli_to_eload_converter import SubCLIToEloadConverter
from eva_submission.eload_preparation import EloadPreparation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def get_submission_id_from_db(project_title):
    """Query eva_submissions.submission_details for a submission_id matching the given project_title."""
    escaped_title = project_title.replace("'", "''")
    query = (
        f"SELECT submission_id FROM eva_submissions.submission_details "
        f"WHERE project_title = '{escaped_title}'"
    )
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        rows = get_all_results_for_query(conn, query)
    if len(rows) == 1:
        return rows[0][0]
    elif len(rows) > 1:
        logger.error(f'Multiple submissions found for project title: {project_title}')
    return None


def add_submission_id_for_eload(eload_num, dry_run=False):
    # Read metadata JSON path and project_title from ELOAD config
    project_title = None
    with EloadPreparation(eload_num) as eload:
        metadata_json_path = eload.eload_cfg.query('submission', 'metadata_json')

    if metadata_json_path and os.path.isfile(metadata_json_path):
        with open(metadata_json_path, 'r') as f:
            json_data = json.load(f)
        project_title = json_data.get('project', {}).get('title')

    if not project_title:
        logger.info(
            f'ELOAD {eload_num}: no metadata JSON or project title found. '
            f'submission is too old to have an existing submission_id, creating a new one'
        )
        submission_id = None
    else:
        # Check DB for existing submission_id
        submission_id = get_submission_id_from_db(project_title)

    # Assign submission_id
    if dry_run:
        if submission_id:
            logger.info(f'ELOAD {eload_num}: dry run — would link to existing submission_id {submission_id}')
        else:
            logger.info(f'ELOAD {eload_num}: dry run — would create a new submission_id via web service')
        return

    if submission_id:
        with SubCLIToEloadConverter(eload_num, submission_id) as eload:
            eload.add_submission_id_to_config()
    else:
        with EloadPreparation(eload_num) as eload:
            eload.add_submission_id_to_config()


def main():
    argparse = ArgumentParser(
        description='Assign a submission_id to each specified ELOAD. '
                    'Checks whether a submission already exists in the database (matched by project_title) '
                    'and links to it; otherwise creates a new submission_id via the web service.'
    )
    argparse.add_argument('--eloads', required=True, type=int, nargs='+',
                          help='One or more ELOAD numbers to process')
    argparse.add_argument('--dry_run', action='store_true', default=False,
                          help='Perform all checks but do not assign the submission_id')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    for eload_num in args.eloads:
        logger.info(f'Processing ELOAD {eload_num}')
        add_submission_id_for_eload(eload_num, dry_run=args.dry_run)


if __name__ == '__main__':
    main()
