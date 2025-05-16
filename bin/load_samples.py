#!/nfs/production/keane/eva/software/eva-submission/development_deployment/EVA3787_load_samples/bin/python3

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
import os
import sys
from argparse import ArgumentParser
from collections import defaultdict
from copy import copy
from functools import cached_property

from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_submission.eload_backlog import EloadBacklog, list_to_sql_in_list
from eva_submission.eload_utils import detect_vcf_aggregation
from eva_submission.evapro.find_from_ena import OracleEnaProjectFinder, ApiEnaProjectFinder
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.samples_checker import get_samples_from_vcf
from eva_submission.submission_config import load_config



def main():
    argparse = ArgumentParser(description='Retrieve information about sample from an ELOAD or Project and load it to EVAPRO')
    argparse.add_argument('--eload', type=int, help='The ELOAD number of the submission for which the samples should be loaded')
    argparse.add_argument('--project_accession', type=str,
                          help='The project accession of the submission for which the samples should be loaded.')
    argparse.add_argument('--clean_up', action='store_true', default=False,
                          help='Remove any downloaded files from the ELOAD directory')
    argparse.add_argument('--print', action='store_true', default=False,
                          help='Print and compare the files and samples from the ENA and EVAPRO')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    argparse.add_argument('--mapping_file', type=str, default=False,
                          help='Allow to pass a file providing a mapping between sample names in ENA and Sample names '
                               'in the VCF files. The format should be one line per mapping with the first column being '
                               'the name in the VCF and the second the name in ENA.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    exit_code = 0
    sample_loader = HistoricalProjectSampleLoader(args.eload, args.project_accession, args.mapping_file)
    if args.print:
        sample_loader.print_sample_matches()
    else:
        exit_code = sample_loader.load_samples()
    if exit_code == 0 and args.clean_up:
        sample_loader.clean_up()
    return exit_code

class HistoricalProjectSampleLoader(EloadBacklog):
    def __init__(self, eload, project_accession, mapping_file):
        super().__init__(eload_number=eload, project_accession=project_accession)
        self.mapping_file = mapping_file
        self.ena_project_finder = OracleEnaProjectFinder()
        self.api_ena_finder = ApiEnaProjectFinder()
        self.eva_project_loader = EvaProjectLoader()
        self.downloaded_files_path = os.path.join(self.eload_dir, '.load_samples_downloaded_files')

    @cached_property
    def sample_mapping(self):
        mapping = {}
        if os.path.isfile(self.mapping_file):
            with open(self.mapping_file) as open_file:
                for line in open_file:
                    sp_line = line.strip().split('\t')
                    mapping[sp_line[0]] = sp_line[1]
        return mapping

    def print_sample_matches(self):
        sample_from_ena_per_analysis = {
            analysis_accession: {
                biosample: ena_sample
                for ena_sample, biosample in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession)
            }
            for analysis_accession in self.analysis_accessions
        }
        file_in_database_per_analysis = {
            analysis_accession: {f.file_md5: f.filename for f in self.eva_project_loader.get_files_for_analysis(analysis_accession)}
            for analysis_accession in self.analysis_accessions
        }
        for analysis_accession in self.analysis_accessions:
            print(f'###  {analysis_accession}  ###')
            # Compare file in ENA/config with file in Database
            header = ['File in ENA', 'md5 in ENA', 'md5 in EVAPRO', 'File in EVAPRO']
            all_rows = []
            files_in_db = copy(file_in_database_per_analysis.get(analysis_accession))
            samples_from_ena = copy(sample_from_ena_per_analysis.get(analysis_accession))
            for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession):
                line = [os.path.basename(vcf_file), md5]
                if md5 in files_in_db:
                    line.append(md5)
                    line.append(files_in_db.pop(md5))
                else:
                    line.append('-')
                    line.append('-')
                all_rows.append(line)
            for md5, vcf_file in files_in_db.items():
                if vcf_file.endswith('.vcf') or vcf_file.endswith('.vcf.gz'):
                    all_rows.append(['-', '-', md5, vcf_file])
            pretty_print(header, all_rows)
            header = ['VCF file', 'Sample in VCF', 'Name in ENA', 'BioSamples', 'ENA sample in analysis']
            all_rows = []

            for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession):
                sample_name_2_accession = copy(self.sample_name_2_accessions_per_analysis.get(analysis_accession))
                sample_names = get_samples_from_vcf(vcf_file)

                for sample_name in sample_names:
                    line = [os.path.basename(vcf_file), sample_name]
                    sample_accession = sample_name_2_accession.get(sample_name)
                    if not sample_accession:
                        sample_name = self.sample_mapping.get(sample_name) or sample_name
                        sample_accession = sample_name_2_accession.get(sample_name)
                    if sample_accession:
                        line.append(sample_name)
                        line.append(sample_name_2_accession.pop(sample_name))
                    else:
                        line.append('-')
                        line.append('-')
                    if sample_accession in samples_from_ena:
                        line.append(samples_from_ena.pop(sample_accession))
                    else:
                        line.append('-')
                    all_rows.append(line)
                for sample_name in sample_name_2_accession:
                    line = ['-', '-', sample_name, sample_name_2_accession.get(sample_name)]
                    if sample_name_2_accession.get(sample_name) in samples_from_ena:
                        line.append(samples_from_ena.pop(sample_name_2_accession.get(sample_name)))
                    else:
                        line.append('-')
                    all_rows.append(line)
                for sample_accession in samples_from_ena:
                    all_rows.append(['-', '-', '-', sample_accession, samples_from_ena.get(sample_accession)])

            pretty_print(header, all_rows)
            print('')

    def load_samples(self):
        result = True
        for analysis_accession in self.analysis_accessions:
            # Add the sample that exists for this analysis
            self.eva_project_loader.eva_session.begin()
            for ena_sample_accession, biosample_accession in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                self.eva_project_loader.insert_sample(biosample_accession=biosample_accession, ena_accession=ena_sample_accession)
            self.eva_project_loader.eva_session.commit()

            # Add link between sample accession and sample name
            aggregation_type = self.analysis_accession_2_aggregation_type.get(analysis_accession)
            if aggregation_type == 'basic':
                result &= self.eva_project_loader.load_samples_from_analysis(self.sample_name_2_accession, analysis_accession)
            else:
                for vcf_file, vcf_file_md5 in self.analysis_accession_2_file_info.get(analysis_accession):
                    result &= self.eva_project_loader.load_samples_from_vcf_file(self.sample_name_2_accession, vcf_file, vcf_file_md5, self.sample_mapping)
        if not result:
            self.error('Not all the Samples were properly loaded in EVAPRO')
            return 1
        return 0


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
    def sample_name_2_accessions_per_analysis(self):
        """Retrieve the sample to biosample accession map from the ENA API"""
        sample_name_2_accessions_per_analysis = {}
        if self.project_accession:
            sample_name_2_accessions_per_analysis = self.api_ena_finder.find_samples_from_analysis(self.project_accession)
        return sample_name_2_accessions_per_analysis

    @cached_property
    def sample_name_2_accession(self):
        """Retrieve the sample to biosample accession map from the config or from the ENA API"""
        sample_name_2_accession = self.eload_cfg.query('brokering', 'Biosamples', 'Samples', ret_default={})
        if not sample_name_2_accession:
                sample_name_2_accession = {name: accession
                                           for analysis_accession in self.sample_name_2_accessions_per_analysis
                                           for name, accession in
                                           self.sample_name_2_accessions_per_analysis[analysis_accession].items()}
        return sample_name_2_accession

    @cached_property
    def analysis_accession_2_file_info(self):
        """Find the files associated with all the analysis accessions"""
        analysis_accession_2_files = defaultdict(list)
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
                    with open(self.downloaded_files_path, 'a') as open_file:
                        open_file.write(full_path+'\n')
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

    def clean_up(self):
        if os.path.exists(self.downloaded_files_path):
            with open(self.downloaded_files_path, 'r') as open_file:
                for line in open_file:
                    f = line.strip()
                    if os.path.exists(f):
                        os.remove(f)
            os.remove(self.downloaded_files_path)


if __name__ == "__main__":
    sys.exit(main())
