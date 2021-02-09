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
import os
import sys
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Accession and ingest submission data into EVA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--instance', required=True, type=int, help='The instance id to use for accessioning')
    # TODO infer aggregation from vcf files, VEP version & cache version from species
    argparse.add_argument('--aggregation', required=True, type=str, choices=['BASIC', 'NONE'], help='The aggregation type')
    argparse.add_argument('--vep_version', required=True, type=int, help='VEP version to use for annotation')
    argparse.add_argument('--vep_cache_version', required=True, type=int, help='VEP cache version to use for annotation')
    argparse.add_argument('--db_name', required=False, type=str, help='Name of existing variant database in MongoDB')
    argparse.add_argument('--tasks', required=False, type=str, nargs='+',
                          default=EloadIngestion.all_tasks, choices=EloadIngestion.all_tasks,
                          help='task or set of tasks to perform during ingestion')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    ingestion = EloadIngestion(args.eload)
    ingestion.ingest(
        aggregation=args.aggregation,
        instance_id=args.instance,
        vep_version=args.vep_version,
        vep_cache_version=args.vep_cache_version,
        db_name=args.db_name,
        tasks=args.tasks
    )


if __name__ == "__main__":
    main()
