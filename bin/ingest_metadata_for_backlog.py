#!/nfs/production/keane/eva/software/eva-submission/production_deployment/v1.19.10/bin/python3

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
from copy import copy

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from eva_submission.eload_ingestion import EloadIngestion
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


class EloadMetadata(EloadIngestion):

    def __init__(self, eload_number, project_accession, analysis_accessions=None, taxonomy=None):
        super().__init__(eload_number=eload_number)
        self._project_accession = project_accession
        if analysis_accessions:
            self._analysis_accessions = analysis_accessions
        else:
            self._analysis_accessions = []
        if taxonomy:
            self._taxonomy = int(taxonomy)
        else:
            self._taxonomy = None

    def ingest(self, tasks=None, vep_cache_assembly_name=None, resume=False):
        if self._analysis_accessions:
            for analysis_accession in self._analysis_accessions:
                self.loader.load_project_from_ena(self._project_accession,
                                                  self.eload_num, analysis_accession,
                                                  taxonomy_id_for_project=self._taxonomy,
                                                  load_browsable_files=True)
        else:
            self.loader.load_project_from_ena(self._project_accession,
                                          self.eload_num,
                                          taxonomy_id_for_project=self._taxonomy,
                                          load_browsable_files=True)

def main():
    argparse = ArgumentParser(description='Ingest the metadata from ENA into EVA')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for this submission.')
    argparse.add_argument('--project_accession', required=True, type=str,
                          help='Specify the project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--analysis_accessions', required=False, type=str, nargs='+',
                          help='Specify the project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--taxonomy', required=False, type=int,
                          help='Specify the taxonomy for this project to be loaded from ENA into EVAPRO')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    # Ingest the metadata but do not create the config for it.
    ingestion = EloadMetadata(args.eload, args.project_accession, args.analysis_accessions, args.taxonomy)
    ingestion.ingest(tasks='metadata_load')

if __name__ == "__main__":
    main()
