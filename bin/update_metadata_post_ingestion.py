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
    argparse = ArgumentParser(description='Update metadata after study has been ingested')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    with EloadIngestion(args.eload) as ingestion:
        ingestion.upgrade_to_new_version_if_needed()
        ingestion.update_assembly_set_in_analysis()
        ingestion.loader.insert_browsable_files_for_project(ingestion.project_accession)
        ingestion.loader.mark_release_browsable_files_for_project(ingestion.project_accession)
        ingestion.loader.update_files_with_ftp_path_for_project(ingestion.project_accession)
        ingestion.loader.refresh_study_browser()
        ingestion.loader.update_loaded_assembly_in_browsable_files_for_project(ingestion.project_accession)
        ingestion.check_assembly_set_id_coherence()


if __name__ == "__main__":
    main()
