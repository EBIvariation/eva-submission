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
import os
import sys
import urllib
from argparse import ArgumentParser
from collections import defaultdict
from copy import copy
from functools import cached_property, lru_cache
from itertools import zip_longest

from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_submission.eload_backlog import EloadBacklog, list_to_sql_in_list
from eva_submission.eload_utils import detect_vcf_aggregation, download_file
from eva_submission.evapro.find_from_ena import OracleEnaProjectFinder, ApiEnaProjectFinder
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.sample_utils import get_samples_from_vcf
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
    argparse.add_argument('--possible_mapping_file', type=str, default=False,
                          help='path to an output file where the possible mapping between unmatched samples is provided. '
                               'This only contains the list of unmatched samples sorted for each analysis')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()
    exit_code = 0
    sample_loader = HistoricalProjectSampleLoader(args.eload, args.project_accession, args.mapping_file, args.possible_mapping_file)
    if args.print:
        sample_loader.print_sample_matches()
    else:
        exit_code = sample_loader.load_samples()
    if exit_code == 0 and args.clean_up:
        sample_loader.clean_up()
    return exit_code

class HistoricalProjectSampleLoader(EloadBacklog):
    def __init__(self, eload, project_accession, mapping_file, possible_mapping_file):
        super().__init__(eload_number=eload, project_accession=project_accession)
        self.mapping_file = mapping_file
        self.possible_mapping_file = possible_mapping_file
        self.ena_project_finder = OracleEnaProjectFinder()
        self.api_ena_finder = ApiEnaProjectFinder()
        self.eva_project_loader = EvaProjectLoader()
        self.downloaded_files_path = os.path.join(self.eload_dir, '.load_samples_downloaded_files')

    @cached_property
    def sample_mapping(self):
        """This file provides the mapping between the sample names in the VCF files and the sample name in the ENA
        or the sample name in the VCF and the biosample accession."""
        mapping = {}
        if os.path.isfile(self.mapping_file):
            with open(self.mapping_file) as open_file:
                for line in open_file:
                    sp_line = line.strip().split('\t')
                    mapping[sp_line[0]] = {}
                    if len(sp_line) > 1:
                        mapping[sp_line[0]]['sample_name'] = sp_line[1]
                    if len(sp_line) > 2:
                        mapping[sp_line[0]]['biosample_accession'] = sp_line[2]

        return mapping

    @lru_cache(maxsize=None)
    def get_samples_from_vcf(self, vcf_path):
        return get_samples_from_vcf(vcf_path)

    def print_sample_matches(self):
        output_mapping = None
        if self.possible_mapping_file:
            output_mapping = open(self.possible_mapping_file, 'w')

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
            print('## File matches:')
            header = ['File in ENA', 'md5 in ENA', 'nb sample in file', 'md5 in EVAPRO', 'File in EVAPRO']
            all_rows = []
            files_in_db = copy(file_in_database_per_analysis.get(analysis_accession))
            samples_from_ena = copy(sample_from_ena_per_analysis.get(analysis_accession))
            for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession, []):
                line = [os.path.basename(vcf_file), str(md5), str(len(self.get_samples_from_vcf(vcf_file)))]
                if md5 in files_in_db:
                    line.append(str(md5))
                    line.append(str(files_in_db.pop(md5)))
                else:
                    line.append('-')
                    line.append('-')
                all_rows.append(line)
            for md5, vcf_file in files_in_db.items():
                if vcf_file.endswith('.vcf') or vcf_file.endswith('.vcf.gz'):
                    all_rows.append(['-', '-', '-', str(md5), str(vcf_file)])
            pretty_print(header, all_rows)

            print('## Sample matches:')
            header = ['VCF file', 'Sample in VCF', 'Name in ENA', 'BioSamples', 'ENA sample in analysis']
            all_rows = []
            unmatched_names_in_ENA = []
            unmatched_name_in_VCF = []
            unmatched_sample_accession = []
            for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession, []):
                sample_name_2_accession = copy(self.sample_name_2_accessions_per_analysis.get(analysis_accession))
                sample_names = self.get_samples_from_vcf(vcf_file)

                for sample_name in sample_names:
                    line = [os.path.basename(vcf_file), sample_name]
                    sample_accession = sample_name_2_accession.get(sample_name)
                    if not sample_accession:
                        mapping = self.sample_mapping.get(sample_name, {})
                        if 'sample_name' in mapping:
                            sample_name = mapping['sample_name']
                        sample_accession = sample_name_2_accession.get(sample_name)
                        if 'biosample_accession' in mapping:
                            sample_accession = mapping['biosample_accession']
                    if sample_accession:
                        line.append(sample_name)
                        if sample_name in sample_name_2_accession:
                            line.append(sample_name_2_accession.pop(sample_name))
                        else:
                            line.append(sample_accession)
                    else:
                        unmatched_name_in_VCF.append(sample_name)
                        line.append('-')
                        line.append('-')
                    if sample_accession in samples_from_ena:
                        line.append(samples_from_ena.pop(sample_accession))
                    else:
                        line.append('-')
                    all_rows.append(line)
                for sample_name in sample_name_2_accession:
                    unmatched_names_in_ENA.append(sample_name)
                    line = ['-', '-', sample_name, sample_name_2_accession.get(sample_name)]
                    if sample_name_2_accession.get(sample_name) in samples_from_ena:
                        line.append(samples_from_ena.pop(sample_name_2_accession.get(sample_name)))
                    else:
                        line.append('-')
                    all_rows.append(line)
                for sample_accession in samples_from_ena:
                    all_rows.append(['-', '-', '-', sample_accession, samples_from_ena.get(sample_accession)])
                    unmatched_sample_accession.append(sample_accession)

            pretty_print(header, all_rows)
            all_rows = []
            print('## Potential mapping of remainging samples')
            header = ['Sample in VCF', 'ENA sample in analysis', 'BioSample accession', 'Resolved']
            resolved_mappings, unmatched_name_in_VCF, unmatched_names_in_ENA = self.resolve_unmatched_samples(unmatched_name_in_VCF, unmatched_names_in_ENA, unmatched_sample_accession)
            for n1, n2, n3, match_type in resolved_mappings:
                all_rows.append([n1, n2, n3, match_type])
                if output_mapping:
                    output_mapping.write(f'{n1}\t{n2}\t{n3}\t{match_type}\n')
            for n1, n2, n3 in zip_longest(sorted(unmatched_name_in_VCF), sorted(unmatched_names_in_ENA), unmatched_sample_accession, fillvalue=''):
                all_rows.append([n1, n2, n3, 'No'])
                if output_mapping:
                    output_mapping.write(f'{n1}\t{n2}\t{n3}\n')
            pretty_print(header, all_rows)

        if output_mapping:
            output_mapping.close()


    def load_samples(self):
        result = True
        for analysis_accession in self.analysis_accessions:
            # Add the sample that exists for this analysis
            self.eva_project_loader.begin_or_continue_transaction()
            for ena_sample_accession, biosample_accession in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                self.eva_project_loader.insert_sample(biosample_accession=biosample_accession, ena_accession=ena_sample_accession)
            self.eva_project_loader.eva_session.commit()

            # Add link between sample accession and sample name
            aggregation_type = self.analysis_accession_2_aggregation_type.get(analysis_accession)
            if aggregation_type == 'basic':
                result &= self.eva_project_loader.load_samples_from_analysis(self.sample_name_2_accession, analysis_accession)
            else:
                for vcf_file, vcf_file_md5 in self.analysis_accession_2_file_info.get(analysis_accession, []):
                    result &= self.eva_project_loader.load_samples_from_vcf_file(
                        self.sample_name_2_accession, vcf_file, vcf_file_md5,
                        analysis_accession=analysis_accession,
                        sample_mapping=self.sample_mapping)
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
        if analysis_accession_dict is not None and isinstance(analysis_accession_dict, dict):
            return analysis_accession_dict.values()
        else:
            return super().analysis_accessions

    @cached_property
    def sample_name_2_accessions_per_analysis(self):
        """Retrieve the sample to biosample accession map from the ENA API"""
        sample_name_2_accessions_per_analysis = {}
        sample_accessions_per_analysis = {}
        # Check the XML first
        if self.analysis_accessions:
            for analysis_accession in self.analysis_accessions:
                tmp = self.api_ena_finder.find_samples_from_analysis_xml(analysis_accession)
                if tmp.get(analysis_accession):
                    sample_accessions_per_analysis.update(tmp)
        # If it does not yield anything then check the filereport
        if not sample_accessions_per_analysis:
            if self.project_accession:
                sample_accessions_per_analysis = self.api_ena_finder.find_samples_from_analysis(self.project_accession)
        # reverse the dictionary where sample name become the key and sample accession the value
        for analysis_accession in sample_accessions_per_analysis:
            sample_name_2_accessions_per_analysis[analysis_accession] = {
                alias: accession for accession, alias in sample_accessions_per_analysis[analysis_accession]
            }
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
        try:
            if self.eload_cfg.query('brokering', 'analyses'):
                # Assume that all the information in contained in the config and the files exist
                for analysis_alias in self.eload_cfg.query('brokering', 'analyses'):
                    vcf_file_dict = self.eload_cfg.query('brokering', 'analyses', analysis_alias, 'vcf_files')
                    analysis_accession = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS', analysis_alias)
                    vcf_info_list = []
                    for vcf_file, vcf_info in vcf_file_dict.items():
                        if os.path.exists(vcf_file):
                            vcf_info_list.append((vcf_file, vcf_info.get('md5')))
                        else:
                            self.error(f'File {vcf_file} cannot be found')
                            raise FileNotFoundError
                    analysis_accession_2_files[analysis_accession] = vcf_info_list
                return analysis_accession_2_files
        except FileNotFoundError:
            # reset the file search as some files were missing
            analysis_accession_2_files = defaultdict(list)

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
            self.info(f'Searching for {filename} in local, ENA and EVA for analysis accession {analysis_accession}')
            full_path = None
            try:
                full_path = self.find_local_file(filename)
            except FileNotFoundError:
                pass
            if not full_path:
                try:
                    full_path = self.find_file_on_ena(filename, analysis_accession)
                except FileNotFoundError:
                    pass
            if not full_path:
                # let the exception be raised if the file is not found
                full_path = self.find_file_on_eva_ftp(filename)
                with open(self.downloaded_files_path, 'a') as open_file:
                    open_file.write(full_path+'\n')
            analysis_accession_2_files[analysis_accession].append((full_path, file_md5))
        return analysis_accession_2_files

    @cached_property
    def analysis_accession_2_aggregation_type(self):
        """Resolve aggregation types for each analysis. Either use the config or the database information"""
        analysis_accession_2_aggregation_type = {}
        if self.eload_cfg.query('ingestion', 'aggregation'):
            analysis_accession_2_aggregation_type = self.eload_cfg.query('ingestion', 'aggregation')
        if not analysis_accession_2_aggregation_type or not isinstance(analysis_accession_2_aggregation_type, dict):
            analysis_accession_2_aggregation_type = {}
            analysis_info = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS')
            if analysis_info and isinstance(analysis_info, dict):
                for analysis_alias, accession in analysis_info.items():
                    analysis_accession_2_aggregation_type[accession] = self.eload_cfg.query('validation', 'aggregation_check', 'analyses', analysis_alias)
        if not analysis_accession_2_aggregation_type or not isinstance(analysis_accession_2_aggregation_type, dict):
            analysis_accession_2_aggregation_type = {}
            for analysis_accession in self.analysis_accessions:
                aggregation_type_per_file = {}
                for vcf_file, md5 in self.analysis_accession_2_file_info.get(analysis_accession, []):
                    aggregation_type_per_file[vcf_file] = detect_vcf_aggregation(vcf_file)
                if len(set(aggregation_type_per_file.values())) == 1:
                    aggregation_type = set(aggregation_type_per_file.values()).pop()
                else:
                    self.error(f'Aggregation type could not be determined for {analysis_accession}.')
                    aggregation_type = None
                analysis_accession_2_aggregation_type[analysis_accession] = aggregation_type
        return analysis_accession_2_aggregation_type

    def find_file_on_eva_ftp(self, filename):
        http_base_url  = f'https://ftp.ebi.ac.uk/pub/databases/eva/{self.project_accession}/'
        basename = os.path.basename(filename)
        full_path = os.path.join(self._get_dir('vcf'), basename)
        if not os.path.exists(full_path):
            try:
                self.info(f'Retrieve {basename} in {self.project_accession} from EVA ftp')
                http_url = http_base_url + basename
                download_file(http_url, full_path)
            except urllib.error.URLError:
                self.error(f'Could not access {http_url} on EVA: most likely does not exist')
                raise FileNotFoundError(f'File not found: {full_path}')
        return full_path

    def clean_up(self):
        if os.path.exists(self.downloaded_files_path):
            with open(self.downloaded_files_path, 'r') as open_file:
                for line in open_file:
                    f = line.strip()
                    if os.path.exists(f):
                        os.remove(f)
            os.remove(self.downloaded_files_path)

    @lru_cache(maxsize=2000)
    def get_existing_biosamples(self, biosample_accession):

        try:
            communicator = WebinHALCommunicator(
                cfg.query('biosamples', 'webin_url'), cfg.query('biosamples', 'bsd_url'),
                cfg.query('biosamples', 'webin_username'), cfg.query('biosamples', 'webin_password')
            )
            sample_json = communicator.follows_link('samples', join_url=biosample_accession)
            return sample_json
        except Exception as e:
            self.error(f'Error retrieving Biosample {biosample_accession}')
            self._logger.exception(e)
        return None

    def resolve_unmatched_samples(self, unmatched_names_in_VCF, unmatched_names_in_ENA, unmatched_sample_accessions):
        mapping = []
        remaining_name_in_VCF = []
        remaining_name_in_sample_accessions = []
        for name_in_vcf in unmatched_names_in_VCF:
            found = False
            potential_matches_for_name = []
            for name_in_ENA in unmatched_names_in_ENA:
                # search the biosample accession associated with the sample in ENA
                biosample_accession = self.sample_name_2_accession.get(name_in_ENA)
                if biosample_accession:
                    attribute, match_type = self.search_mapping_in_biosample(biosample_accession, name_in_vcf)
                    if attribute and match_type == 'full':
                        self.info(f'{name_in_vcf} in VCF matches {attribute} from {biosample_accession}:{name_in_ENA}')
                        mapping.append((name_in_vcf, name_in_ENA, biosample_accession, 'Matched'))
                        found = True
                        break
                    elif attribute and match_type == 'contained':
                        self.info(f'{name_in_vcf} in VCF is contained in {attribute} from {biosample_accession}:{name_in_ENA}')
                        potential_matches_for_name.append((attribute, name_in_ENA, biosample_accession))
            if not found:
                # Check potential matches
                if len(set((biosample for  _, _, biosample in potential_matches_for_name))) == 1:
                    # only one biosample matches
                    name_in_ENA, biosample_accession = set((name, biosample) for _, name, biosample in potential_matches_for_name).pop()
                    mapping.append((name_in_vcf, name_in_ENA, biosample_accession, 'Partial'))
            if not found:
                remaining_name_in_VCF.append(name_in_vcf)
            else:
                unmatched_names_in_ENA.remove(name_in_ENA)
        return mapping, remaining_name_in_VCF, unmatched_names_in_ENA

    def search_mapping_in_biosample(self, biosample_accession, sample_name, list_of_characteristics=None):
        sample_json = self.get_existing_biosamples(biosample_accession)
        if sample_json and 'characteristics' in sample_json:
            for attribute in sample_json['characteristics']:
                if not list_of_characteristics or attribute in list_of_characteristics:
                    if sample_json['characteristics'][attribute][0]['text'] == sample_name:
                        return attribute, 'full'
            if sample_name in sample_json['name']:
                return 'name', 'contained'
            for attribute in sample_json['characteristics']:
                if not list_of_characteristics or attribute in list_of_characteristics:
                    if sample_name in sample_json['characteristics'][attribute][0]['text']:
                        return attribute, 'contained'

        return None, None


if __name__ == "__main__":
    sys.exit(main())

