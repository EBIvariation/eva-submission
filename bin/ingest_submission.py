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
from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Accession and ingest submission data into EVA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission.')
    argparse.add_argument('--instance', required=False, type=int, choices=range(1, 13), default=1,
                          help='The instance id to use for accessioning. Only needed if running accessioning.')
    argparse.add_argument('--tasks', required=False, type=str, nargs='+',
                          default=EloadIngestion.all_tasks, choices=EloadIngestion.all_tasks,
                          help='Task or set of tasks to perform during ingestion.')
    argparse.add_argument('--vep_cache_assembly_name', required=False, type=str,
                          help='The assembly name used in the VEP cache to help the script to find the correct cache '
                               'to use. This should be only used rarely when the script cannot find the VEP cache but '
                               'we know it exists.')
    argparse.add_argument('--resume', action='store_true', default=False,
                          help='Whether to resume an existing Nextflow process within ingestion.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    with EloadIngestion(args.eload) as ingestion:
        ingestion.upgrade_config_if_needed()
        ingestion.ingest(
            instance_id=args.instance,
            tasks=args.tasks,
            vep_cache_assembly_name=args.vep_cache_assembly_name,
            resume=args.resume
        )


if __name__ == "__main__":
    main()
