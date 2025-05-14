#!/usr/bin/env python

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
from argparse import ArgumentParser
from functools import cached_property

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_submission.eload_backlog import EloadBacklog, list_to_sql_in_list
from eva_submission.eload_utils import detect_vcf_aggregation
from eva_submission.evapro.find_from_ena import OracleEnaProjectFinder, ApiEnaProjectFinder
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.submission_config import load_config



def main():
    argparse = ArgumentParser(description='Retrieve information about sample from an ELOAD or Project and load it to EVAPRO')
    argparse.add_argument('--eload', type=int, help='The ELOAD number of the submission for which the samples should be loaded')
    argparse.add_argument('--project_accession', type=str,
                          help='The project accession of the submission for which the samples should be loaded.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    HistoricalProjectSampleLoader(args.eload, args.project_accession).load_samples()

class HistoricalProjectSampleLoader(EloadBacklog):
    def __init__(self, eload, project_accession):
        super().__init__(eload_number=eload, project_accession=project_accession)
        self.ena_project_finder = OracleEnaProjectFinder()
        self.api_ena_finder = ApiEnaProjectFinder()
        self.eva_project_loader = EvaProjectLoader()

    def load_samples(self):
        # check all the data and retrieve all the files before loading to the database
        self.project_accession
        self.analysis_accessions
        self.sample_name_2_accession
        self.analysis_accession_2_file_info
        self.analysis_accession_2_aggregation_type

        for analysis_accession in self.analysis_accessions:
            # Add the sample that exists for this analysis
            self.eva_project_loader.eva_session.begin()
            for ena_sample_accession, biosample_accession in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                self.eva_project_loader.insert_sample(biosample_accession=biosample_accession, ena_accession=ena_sample_accession)
            self.eva_project_loader.eva_session.commit()

            # Add link between sample accession and sample name
            aggregation_type = self.analysis_accession_2_aggregation_type.get(analysis_accession)
            if aggregation_type == 'basic':
                self.eva_project_loader.load_samples_from_analysis(self.sample_name_2_accession, analysis_accession)
            else:
                for vcf_file, vcf_file_md5 in self.analysis_accession_2_file_info.get(analysis_accession):
                    self.eva_project_loader.load_samples_from_vcf_file(self.sample_name_2_accession, vcf_file, vcf_file_md5)


    @cached_property
    def project_accession(self):
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        if project_accession is not None:
            return project_accession
        else:
            return super().project_accession

    @cached_property
    def analysis_accessions(self):
        analysis_accession_dict = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS')
        if analysis_accession_dict is not None:
            return analysis_accession_dict.values()
        else:
            return super().analysis_accessions

    @cached_property
    def sample_name_2_accession(self):
        """Retrieve the sample to biosample accession map from the config or from the ENA API"""
        sample_name_2_accession = self.eload_cfg.query('brokering', 'Biosamples', 'Samples', ret_default={})
        if not sample_name_2_accession:
            if self.project_accession:
                sample_name_2_accessions_per_analysis = self.api_ena_finder.find_samples_from_analysis(
                    self.project_accession)
                sample_name_2_accession = dict([(name, accession)
                                                 for analysis_accession in sample_name_2_accessions_per_analysis
                                                 for name, accession in
                                                 sample_name_2_accessions_per_analysis[analysis_accession].items()
                                                 ])
        return sample_name_2_accession

    @cached_property
    def analysis_accession_2_file_info(self):
        """Find the files associated with all the analysis accessions"""
        analysis_accession_2_files = {}
        if self.eload_cfg.query('brokering', 'analyses'):
            # Assume that all the information in contained in the config and the files exist
            for analysis_alias in self.eload_cfg.query('brokering', 'analyses'):
                vcf_file_dict = self.eload_cfg.query('brokering', 'analyses', analysis_alias, 'vcf_files')
                analysis_accession = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS', analysis_alias)
                vcf_info_list = [(vcf_file, vcf_info.get('md5')) for vcf_file, vcf_info in vcf_file_dict.items()]
                analysis_accession_2_files[analysis_accession] = vcf_info_list
        else:
            # resolve the files from the database and download if required similar to EloadBacklog.get_analysis_info
            with self.metadata_connection_handle as conn:
                query = f"select a.analysis_accession, c.filename, c.file_md5 " \
                        f"from analysis a " \
                        f"join analysis_file b on a.analysis_accession=b.analysis_accession " \
                        f"join file c on b.file_id=c.file_id " \
                        f"where a.analysis_accession in {list_to_sql_in_list(self.analysis_accessions)};"
                rows = get_all_results_for_query(conn, query)

            for analysis_accession, filename, file_md5 in rows:
                if not filename.endswith('.vcf.gz'):
                    continue
                try:
                    full_path = self.find_local_file(filename)
                except FileNotFoundError:
                    full_path = self.find_file_on_ena(filename, analysis_accession)
                if analysis_accession not in analysis_accession_2_files:
                    analysis_accession_2_files[analysis_accession] = []
                analysis_accession_2_files[analysis_accession].append((full_path, file_md5))
        return analysis_accession_2_files

    @cached_property
    def analysis_accession_2_aggregation_type(self):
        """Resolve aggregation types for each analysis. Either use the config or the database information"""
        analysis_accession_2_aggregation_type={}
        if self.eload_cfg.query('ingestion', 'aggregation'):
            analysis_accession_2_aggregation_type = self.eload_cfg.query('ingestion', 'aggregation')
        if not analysis_accession_2_aggregation_type:
            analysis_info = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS')
            if analysis_info:
                for analysis_alias, accession in analysis_info.item():
                    analysis_accession_2_aggregation_type[accession] = self.eload_cfg.query('validation', 'aggregation_check', 'analyses', analysis_alias)
        if not analysis_accession_2_aggregation_type:
            for analysis_accession in self.analysis_accessions:
                aggregation_type_per_file = {}
                for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession):
                    aggregation_type_per_file[vcf_file] = detect_vcf_aggregation(vcf_file)
                if len(set(aggregation_type_per_file.values())) == 1:
                    aggregation_type = set(aggregation_type_per_file.values()).pop()
                else:
                    self.error(f'Aggregation type could not be determined for {analysis_accession}.')
                    aggregation_type = None
                analysis_accession_2_aggregation_type[analysis_accession] = aggregation_type
        return analysis_accession_2_aggregation_type


if __name__ == "__main__":
    main()
