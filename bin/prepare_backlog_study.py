#!/usr/bin/env python

# Copyright 2021 EMBL - European Bioinformatics Institute
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

from eva_submission.eload_backlog import EloadBacklog
from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    validation_tasks = ['aggregation_check', 'assembly_check', 'vcf_check']
    forced_validation_tasks = ['metadata_check', 'sample_check']

    argparse = ArgumentParser(description='Prepare to process backlog study and validate VCFs.')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--project_accession', required=False, type=str,
                          help='Set this project instead of the one associated with this eload. '
                               'Useful when the association is not set in the database. '
                               'The project needs to exists in the DB.')
    argparse.add_argument('--analysis_accessions', required=False, type=str, nargs='+',
                          help='Set these analysis instead of the ones associated with the project. '
                               'Useful when wanting to use a subset of the analysis. '
                               'The analyses need to exists in the DB.')
    argparse.add_argument('--force_config', action='store_true', default=False,
                          help='Overwrite the configuration file after backing it up.')
    argparse.add_argument('--keep_config', action='store_true', default=False,
                          help='Keep the configuration file as it is and only run the validation on it.')
    argparse.add_argument('--validation_tasks', required=False, type=str, nargs='+',
                          default=validation_tasks, choices=validation_tasks,
                          help='task or set of tasks to perform during validation')
    argparse.add_argument('--merge_per_analysis', action='store_true', default=False,
                          help='Whether to merge vcf files per analysis if possible.')
    argparse.add_argument('--set_as_valid', action='store_true', default=False,
                          help='Set the script to consider all validation tasks performed as valid in the final '
                               'evaluation. This does not affect the actual report but only change the final '
                               'evaluation')
    argparse.add_argument('--report', action='store_true', default=False,
                          help='Set the script to only report the results based on previously run preparation.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    with EloadBacklog(args.eload,
                      project_accession=args.project_accession,
                      analysis_accessions=args.analysis_accessions) as preparation:
        # Pass the eload config object to validation so that the two objects share the same state
        with EloadValidation(args.eload, preparation.eload_cfg) as validation:
            if not args.report and not args.keep_config:
                preparation.fill_in_config(args.force_config)

            if not args.report:
                validation.validate(args.validation_tasks)
                # Also mark the other validation tasks as force so they are all passable

                if args.set_as_valid:
                    forced_validation_tasks = validation.all_validation_tasks
                for validation_task in forced_validation_tasks:
                    validation.eload_cfg.set('validation', validation_task, 'forced', value=True)
                validation.mark_valid_files_and_metadata(args.merge_per_analysis)
                if args.merge_per_analysis:
                    preparation.copy_valid_config_to_brokering_after_merge()

            preparation.report()
            validation.report()
            logger.info('Preparation complete, if files are valid please run ingestion as normal.')


if __name__ == "__main__":
    main()
