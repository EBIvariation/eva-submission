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

from eva_submission.eload_brokering import EloadBrokering
from eva_submission.eload_utils import check_existing_project
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def ENA_Project(project):
    """Helper function to validate early that the project provided exist in ENA and is public"""
    if not check_existing_project(str(project)):
        logger.warning(f'Project {project} provided does not exist in ENA.')
        raise ValueError
    return str(project)


def main():
    argparse = ArgumentParser(description='Broker validated ELOAD to BioSamples and ENA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    argparse.add_argument('--vcf_files', required=False, type=str, help='VCF files to use in the brokering', nargs='+')
    argparse.add_argument('--metadata_file', required=False, type=str, help='VCF files to use in the brokering')
    argparse.add_argument('--project_accession', required=False, type=ENA_Project,
                          help='Use this option to set an existing project accession that will be used to attach the '
                               'new analyses from this ELOAD.')
    argparse.add_argument('--use_async_upload',  action='store_true', default=False,
                          help='Change the mode of upload to ENA to use the async queue.')
    argparse.add_argument('--force', required=False, type=str, nargs='+', default=[],
                          choices=EloadBrokering.all_brokering_tasks,
                          help='When not set, the script only performs the tasks that were not successful. Can be '
                               'set to specify one or several tasks to force during the brokering regardless of '
                               'previous status')
    argparse.add_argument('--report', action='store_true', default=False,
                          help='Set the script to only report the results based on previously run brokering.')

    log_cfg.add_stdout_handler()
    args = argparse.parse_args()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    # Optionally Set the valid VCF and metadata file
    with EloadBrokering(args.eload, args.vcf_files, args.metadata_file) as brokering:
        brokering.upgrade_config_if_needed()
        if not args.report:
            brokering.broker(brokering_tasks_to_force=args.force, existing_project=args.project_accession,
                             async_upload=args.use_async_upload)
        brokering.report()


if __name__ == "__main__":
    main()
