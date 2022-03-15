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

from eva_submission.ena_retrieval import retrieve_files_from_ena
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Retrieve file information from ENA and add them to EVAPRO. '
                                          'Remove extra vcf and index files in EVAPRO is they are not in ENA')
    argparse.add_argument('--project_accession', required=False, type=str,
                          help='Specify the project accession for which the retrieval should be done. This will apply to the whole projetc')
    argparse.add_argument('--analysis_accession', required=False, type=str,
                          help='Specify the analysis accession for which the retrieval should be done.')

    log_cfg.add_stdout_handler()
    args = argparse.parse_args()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    if args.analysis_accession:
        retrieve_files_from_ena(args.analysis_accession)
    elif args.project_accession:
        retrieve_files_from_ena(args.project_accession)


if __name__ == "__main__":
    main()
