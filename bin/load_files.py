#!/bin/env python3

# Copyright 2025 EMBL - European Bioinformatics Institute
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

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.eload_backlog import EloadBacklog
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.submission_config import load_config



def main():
    argparse = ArgumentParser(description='Retrieve information about VCF files in the config from an ELOAD or and load it to EVAPRO')
    argparse.add_argument('--eload', type=int, help='The ELOAD number of the submission for which the samples should be loaded')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    exit_code = 0
    file_loader = HistoricalProjectFileLoader(args.eload)
    file_loader.load_files_from_config()

    return exit_code

class HistoricalProjectFileLoader(EloadBacklog):
    def __init__(self, eload):
        super().__init__(eload_number=eload)
        self.eva_project_loader = EvaProjectLoader()


    def load_files_from_config(self):
        taxonomy_id=self.eload_cfg.query('submission', 'taxonomy_id')
        for analysis_alias in self.eload_cfg.query('validation', 'aggregation_check', 'analyses'):
            analysis_accession = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS', analysis_alias)
            assert analysis_accession in self.analysis_accessions, f'Analysis {analysis_accession} is not in EVAPRO'
            analysis_info = self.eload_cfg.query('brokering', 'analyses', analysis_alias)
            for vcf_file in analysis_info.get('vcf_files'):
                if 'md5' not in analysis_info.get('vcf_files'):
                    # create the md5 of the vcf
                    run_command_with_output(f'calculate the md5 for {vcf_file}',
                                            f'md5sum {vcf_file} > {vcf_file}.md5')
                    with open(f'{vcf_file}.md5') as open_file:
                        md5_value = open_file.readline().strip().split()[0]
                        self.eload_cfg.set('brokering', 'analyses',analysis_alias,vcf_file,'md5', value=md5_value)
            self.eva_project_loader.load_vcf_files_from_config(
                project_accession=self.project_accession, analysis_accession=analysis_accession,
                taxonomy_id=taxonomy_id, assembly_accession=analysis_info.get('assembly_accession'),
                vcf_file_dict=analysis_info.get('vcf_files')
            )

if __name__ == "__main__":
    sys.exit(main())

