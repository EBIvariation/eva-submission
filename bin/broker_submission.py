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
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Broker validated ELOAD to BioSamples and ENA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    argparse.add_argument('--vcf_files', required=False, type=str, help='VCF files to use in the brokering', nargs='+')
    argparse.add_argument('--metadata_file', required=False, type=str, help='VCF files to use in the brokering')
    argparse.add_argument('--force', required=False, type=str, nargs='+', default=[],
                          choices=EloadBrokering.all_brokering_tasks,
                          help='When not set, the script only performs the tasks that were not successful. Can be '
                               'set to specify one or several tasks to force during the brokering regardless of '
                               'previous status')
    argparse.add_argument('--report', action='store_true', default=False,
                          help='Set the script to only report the results based on previously run brokering.')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    # Optionally Set the valid VCF and metadata file
    brokering = EloadBrokering(args.eload, args.vcf_files, args.metadata_file)
    brokering.upgrade_config_if_needed()
    if not args.report:
        brokering.broker(brokering_tasks_to_force=args.force)
    brokering.report()


if __name__ == "__main__":
    main()
