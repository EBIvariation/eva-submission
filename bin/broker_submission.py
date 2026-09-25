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
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing import sub_cli_utils
from eva_submission.submission_brokering import SubmissionBrokering
from eva_submission.submission_utils import check_existing_project_in_ena
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def ENA_Project(project):
    """Helper function to validate early that the project provided exist in ENA and is public"""
    if not check_existing_project_in_ena(str(project)):
        logger.warning(f'Project {project} provided does not exist in ENA.')
        raise ValueError
    return str(project)


def main():
    argparse = ArgumentParser(description='Broker validated Submission to BioSamples and ENA')
    argparse.add_argument('--submission_id', required=True, type=str, help='Submission ID of the submission')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    argparse.add_argument('--project_accession', required=False, type=ENA_Project,
                          help='Use this option to set an existing project accession that will be used to attach the '
                               'new analyses from this Submission.')
    argparse.add_argument('--use_legacy_upload', action='store_true', default=False,
                          help='Change the mode of upload to ENA to use the version 1 metadata upload instead of the async queue.')
    argparse.add_argument('--dry_ena_upload', action='store_true', default=False,
                          help='Prevent the upload of files to ENA FTP and XML files to submission.')
    argparse.add_argument('--output_format', choices=['xml', 'json'], default='xml',
                          help='Format of the files that will be sent to ENA for the brokering.')
    argparse.add_argument('--force', required=False, type=str, nargs='+', default=[],
                          choices=SubmissionBrokering.all_brokering_tasks,
                          help='When not set, the script only performs the tasks that were not successful. Can be '
                               'set to specify one or several tasks to force during the brokering regardless of '
                               'previous status')
    argparse.add_argument('--report', action='store_true', default=False,
                          help='Set the script to only report the results based on previously run brokering.')
    argparse.add_argument('--nextflow_config', type=str, required=False,
                          help='Path to the configuration file that will be applied to the Nextflow process. '
                               'This will override other nextflow configuration files on the filesystem')
    argparse.add_argument('--resume', action='store_true', default=False,
                          help='Whether to resume an existing Nextflow process within brokering preparation.')

    log_cfg.add_stdout_handler()
    args = argparse.parse_args()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    submission = sub_cli_utils.fetch_submission(args.submission_id)
    if not submission:
        logger.error(f'Submission {args.submission_id} not found')
        sys.exit(1)

    with SubmissionBrokering(args.submission_id, nextflow_config=args.nextflow_config) as brokering:
        brokering.upgrade_to_new_version_if_needed()
        if not args.report:
            try:
                brokering.broker(brokering_tasks_to_force=args.force, existing_project=args.project_accession,
                                 async_upload=not args.use_legacy_upload, dry_ena_upload=args.dry_ena_upload,
                                 output_format=args.output_format, resume=args.resume)
            except Exception as e:
                brokering.update_submission_status(sub_cli_utils.BROKERING, sub_cli_utils.FAILURE)
                raise e
        brokering.report()


if __name__ == "__main__":
    main()
