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
import argparse
import logging
import os
import sys

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.assembly import NCBIAssembly

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from eva_submission.submission_config import load_config


logger = log_cfg.get_logger(__name__)


def main(assembly_accession, species_name, output_directory, clear):
    output_directory = output_directory or cfg.query('genome_downloader', 'output_directory')
    assembly = NCBIAssembly(assembly_accession, species_name, output_directory, eutils_api_key=cfg['eutils_api_key'])
    assembly.download_or_construct(overwrite=clear)
    logger.info(assembly.assembly_fasta_path)
    logger.info(assembly.assembly_report_path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Genome downloader assembly', add_help=False)
    parser.add_argument("-a", "--assembly-accession",
                        help="Assembly for which the process has to be run, e.g. GCA_000002285.2", required=True)
    parser.add_argument("-s", "--species",
                        help="Species scientific name under which this accession should be stored. "
                             "This is only used to create the directory", required=True)
    parser.add_argument("-o", "--output-directory",
                        help="Base directory under which all species assemblies are stored. "
                             "Will use the one defined in config file if omitted")
    parser.add_argument("-c", "--clear", help="Flag to clear existing data in FASTA file and starting from scratch",
                        action='store_true')
    parser.add_argument('--help', action='help', help='Show this help message and exit')
    parser.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = parser.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    try:
        main(args.assembly_accession, args.species, args.output_directory, args.clear)
    except Exception as ex:
        logger.exception(ex)
        sys.exit(1)

    sys.exit(0)
