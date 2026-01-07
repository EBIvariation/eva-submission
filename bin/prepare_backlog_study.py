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

from eva_submission.eload_backlog import EloadBacklog, EloadMetadataForBacklog
from eva_submission.eload_brokering import EloadBrokering
from eva_submission.eload_validation import EloadValidation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    possible_validation_tasks = ['vcf_check']
    forced_validation_tasks = list(set(EloadValidation.all_validation_tasks) - set(possible_validation_tasks))
    argparse = ArgumentParser(description='Prepare to process backlog study and validate VCFs.')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--project_accession', required=True, type=str,
                          help='Specify the project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--analysis_accessions', required=False, type=str, nargs='+',
                          help='Specify the project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--taxonomy', required=False, type=int,
                          help='Specify the taxonomy for this project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--force_config', action='store_true', default=False,
                          help='Overwrite the configuration file after backing it up.')
    argparse.add_argument('--keep_config', action='store_true', default=False,
                          help='Keep the configuration file as it is and only run the validation on it.')
    argparse.add_argument('--validation_tasks', required=False, type=str, nargs='+',
                          default=possible_validation_tasks, choices=possible_validation_tasks,
                          help='task or set of tasks to perform during validation')
    argparse.add_argument('--shallow_validation', action='store_true', default=False,
                          help='Set the validation to be performed on the first 10000 records of the VCF. '
                               'Only applied if the number of records exceed 10000')
    argparse.add_argument('--report', action='store_true', default=False,
                          help='Set the script to only report the results based on previously run preparation.')
    argparse.add_argument('--nextflow_config', type=str, required=False,
                          help='Path to the configuration file that will be applied to the Nextflow process. '
                               'This will override other nextflow configuration files on the filesystem')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    if args.project_accession:
        logger.info(f'Load Metadata using project accession: {args.project_accession} analysis accessions: {args.analysis_accessions} and taxonomy: {args.taxonomy}' )
        # Ingest the metadata but do not create the config for it.
        ingestion = EloadMetadataForBacklog(args.eload, args.project_accession, args.analysis_accessions, args.taxonomy)
        ingestion.ingest(tasks='metadata_load')

    with EloadBacklog(args.eload,
                      project_accession=args.project_accession,
                      analysis_accessions=args.analysis_accessions) as preparation:
        if not args.report and not args.keep_config:
            preparation.fill_in_config(args.force_config)


    # # Pass the eload config object to validation so that the two objects share the same state
    with EloadValidation(args.eload, preparation.eload_cfg, nextflow_config=args.nextflow_config) as validation:
        validation.set_validation_task_result_valid(forced_validation_tasks)
        validation.validate(validation_tasks=args.validation_tasks)

    # Stop the processing if the validation did not pass
    if not validation.eload_cfg.query('validation', 'valid', 'analyses'):
        raise ValueError('Cannot proceed to the Ingestion preparation because one of the validation did not pass.')

    with EloadBrokering(args.eload, config_object=preparation.eload_cfg, nextflow_config=args.nextflow_config) as eload_brokering:
        eload_brokering.prepare_brokering()
        for key in ['ena', 'Biosamples']:
            eload_brokering.eload_cfg.set('brokering', key, 'pass', value=True)

    preparation.report()
    # validation.report()
    eload_brokering.report()

    logger.info('Preparation complete, if files are valid please run ingestion as normal.')


if __name__ == "__main__":
    main()
