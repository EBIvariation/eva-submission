#!/usr/bin/env python

# Copyright 2021 EMBL - European Bioinformatics Institute
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
from argparse import ArgumentParser
from functools import cached_property
from urllib.error import URLError

import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query
from eva_submission.eload_utils import detect_vcf_aggregation, download_file
from eva_submission.evapro.find_from_ena import EnaProjectFinder
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.submission_config import load_config, EloadConfig

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Retrieve information about sample from an ELOAD or Project and load it to EVAPRO')
    argparse.add_argument('--eload', type=int, help='The ELOAD number of the submission for which the samples should be loaded')
    argparse.add_argument('--project_accession', type=str,
                          help='The project accession of the submission for which the samples should be loaded.')
    argparse.add_argument('--analysis_accession', required=False, type=str,
                          help='The analysis accession of the submission for which the samples should be loaded.')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

class HistoricalProjectSampleLoader:
    def __init__(self, eload, project_accession, analysis_accession):
        self.eload = eload
        self.project_accession = project_accession
        self.analysis_accession = analysis_accession

    @cached_property
    def eload_cfg(self):
        if self.eload is not None:
            config_path = os.path.join(cfg['eloads_dir'], self.eload, '.' + self.eload + '_config.yml')
            eload_cfg = EloadConfig(config_path)
            return eload_cfg
        else:
            # config that cannot be saved and should return None to any query
            return EloadConfig()

    @property
    def metadata_connection_handle(self):
        return get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file'])

    def load_samples(self):
        ena_project_finder = EnaProjectFinder()
        eva_project_loader = EvaProjectLoader()

        sample_name_2_accession = retrieve_sample_2_accession()

        for analysis_accession in self.find_analysis_accessions():
            for sample_info in ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                sample_id, sample_accession = sample_info
                eva_project_loader.insert_sample(biosample_accession=sample_accession, ena_accession=sample_id)

            aggregation_type = self.determine_aggregation_type(analysis_accession)
            if aggregation_type == 'basic':
                eva_project_loader.load_samples_from_analysis(sample_name_2_accession, analysis_accession)
            else:
                for vcf_file, vcf_file_md5 in determine_files(analysis_accession):
                    eva_project_loader.load_samples_from_vcf_file(sample_name_2_accession, vcf_file, vcf_file_md5)

    def determine_aggregation_type(self, analysis_accession):
        aggregation_type = self.eload_cfg.query('ingestion', 'aggregation')
        if not aggregation_type:
            analysis_info = self.eload_cfg.query('brokering', 'ena', 'ANALYSIS')
            if analysis_info:
                analysis_alias = [analysis_alias for analysis_alias, accession in analysis_info.item() if accession==analysis_accession]
                if analysis_alias:
                    aggregation_type = self.eload_cfg.query('validation', 'aggregation_check', 'analyses', analysis_alias[0])
        if not aggregation_type:
            aggregation_type_per_file = {}
            for vcf_file in self.get_files_for_analysis_accession(analysis_accession):
                aggregation_type_per_file[vcf_file] = detect_vcf_aggregation(vcf_file)
            if len(set(aggregation_type_per_file.values())) == 1:
                aggregation_type = set(aggregation_type_per_file.values()).pop()
            else:
                logger.error(f'Aggregation type could not be determined for {analysis_accession}.')
                aggregation_type = None
        return aggregation_type

    def vcf_files_for_analysis_accession(self, analysis_accession):
        with self.metadata_connection_handle as conn:
            query = f"select c.filename " \
                    f"from analysis a " \
                    f"join analysis_file b on a.analysis_accession=b.analysis_accession " \
                    f"join file c on b.file_id=c.file_id " \
                    f"where a.analysis_accession = '{analysis_accession}';"
            rows = get_all_results_for_query(conn, query)
        return [filename for filename, in rows if filename.endswith('.vcf.gz')]

    def get_vcf_file_from_ena(self, analysis_accession):
        """
        Find and download all the VCF files for this analysis accession based on the ones associated with the
        analysis accession in EVAPRO and the actual files present in the ENA FTP.
        """
        vcf_file_list = []
        for fn in self.vcf_files_for_analysis_accession(analysis_accession):
            full_path = self.find_file_on_ena(fn, analysis_accession)
            vcf_file_list.append(full_path)
        return vcf_file_list

    def find_file_on_ena(self, fn, analysis):
        basename = os.path.basename(fn)
        full_path = os.path.join(self.work_directory, basename)
        if not os.path.exists(full_path):
            try:
                ftp_urls = self._get_files_from_ena_analysis(analysis)
                urls = [ftp_url for ftp_url in ftp_urls if ftp_url.endswith(fn)]
                if len(urls) == 1:
                    url = 'https://' + urls[0]
                    download_file(url, full_path)
                else:
                    logger.error(f'Could find {fn} in analysis {analysis} on ENA: most likely does not exist')
                    raise FileNotFoundError(f'File not found: {full_path}')
            except URLError:
                logger.error(f'Could not access {url} on ENA: most likely does not exist')
                raise FileNotFoundError(f'File not found: {full_path}')
        return full_path

    def _get_files_from_ena_analysis(self, analysis_accession):
        """Find the location of the file submitted with an analysis"""
        analyses_url = (
            f"https://www.ebi.ac.uk/ena/portal/api/filereport?result=analysis&accession={analysis_accession}"
            f"&format=json&fields=submitted_ftp"
        )
        response = requests.get(analyses_url)
        response.raise_for_status()
        data = response.json()
        if data:
            return data[0].get('submitted_ftp').split(';')
        else:
            return {}

    def get_files_for_analysis_accession(self, analysis_accession):
        self.eload_cfg.query('brokering', 'ena', 'ANALYSIS')


    def retrieve_sample_2_accession(self):
        sample_name_2_accession = self.eload_cfg.query('brokering', 'Biosamples', 'Samples')
        if not(sample_name_2_accession):




def determine_files(analysis_accession):
    pass


if __name__ == "__main__":
    main()
