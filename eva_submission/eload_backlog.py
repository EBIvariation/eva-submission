import os
import urllib

import requests
from cached_property import cached_property
from ebi_eva_internal_pyutils.pg_utils import get_all_results_for_query

from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import get_reference_fasta_and_report, get_project_alias, download_file
from eva_submission.submission_config import EloadConfig


def list_to_sql_in_list(l):
    """Convert a python list into a string that can be used in an SQL query with operator "in" """
    return '(' + ','.join(f"'{e}'" for e in l) + ')'


class EloadBacklog(Eload):

    def __init__(self, eload_number: int, config_object: EloadConfig = None, project_accession: str = None,
                 analysis_accessions: list = None):
        super().__init__(eload_number, config_object)
        self._preset_project_accession = project_accession
        self._preset_analysis_accessions = analysis_accessions

    def fill_in_config(self, force_config=False):
        """Fills in config params from metadata DB and ENA, enabling later parts of pipeline to run."""
        if not self.eload_cfg.is_empty() and not force_config:
            self.error(f'Already found a config file for {self.eload} while running backlog preparation')
            self.error('Please remove the existing config file and try again.')
            raise ValueError(f'Already found a config file for {self.eload} while running backlog preparation')
        elif not self.eload_cfg.is_empty():
            # backup the previous config and remove the existing content
            self.eload_cfg.backup()
            self.eload_cfg.clear()
        self.eload_cfg.set('brokering', 'ena', 'PROJECT', value=self.project_accession)
        self.get_analysis_info()
        self.get_species_info()
        self.update_config_with_hold_date(self.project_accession, self.project_alias)
        self.eload_cfg.write()

    @cached_property
    def project_accession(self):
        if self._preset_project_accession:
            with self.metadata_connection_handle as conn:
                query = f"select project_accession from evapro.project " \
                        f"where project_accession='{self._preset_project_accession}';"
                rows = get_all_results_for_query(conn, query)
            if len(rows) != 1:
                raise ValueError(f'No project found for {self._preset_project_accession} in metadata DB.')
        else:
            with self.metadata_connection_handle as conn:
                query = f"select project_accession from evapro.project_eva_submission where eload_id={self.eload_num};"
                rows = get_all_results_for_query(conn, query)
            if len(rows) != 1:
                raise ValueError(f'No project accession for {self.eload} found in metadata DB.')
        return rows[0][0]

    @cached_property
    def analysis_accessions(self):
        if self._preset_analysis_accessions:
            with self.metadata_connection_handle as conn:
                query = (f"select distinct analysis_accession from analysis "
                         f"where analysis_accession in {list_to_sql_in_list(self._preset_analysis_accessions)}"
                         f" and hidden_in_eva=0;")
                rows = get_all_results_for_query(conn, query)
                if len(rows) != len(self._preset_analysis_accessions):
                    raise ValueError(f"Some analysis accession could not be found for analyses "
                                     f"{', '.join(self._preset_analysis_accessions)} in metadata DB.")
        else:
            with self.metadata_connection_handle as conn:
                query = (f"select distinct b.analysis_accession from project_analysis a "
                         f"join analysis b on a.analysis_accession=b.analysis_accession "
                         f"where a.project_accession='{self.project_accession}' and b.hidden_in_eva=0;")
                rows = get_all_results_for_query(conn, query)
                if len(rows) == 0:
                    raise ValueError(f'No analysis accession could be found for project {self.project_accession} '
                                     f'in metadata DB.')
        return [row[0] for row in rows]

    @cached_property
    def project_alias(self):
        return get_project_alias(self.project_accession)

    def get_species_info(self):
        """Adds species info into the config: taxonomy id and scientific name,
        and assembly accession, fasta, and report."""
        with self.metadata_connection_handle as conn:
            query = f"select a.taxonomy_id, b.scientific_name " \
                    f"from project_taxonomy a " \
                    f"join taxonomy b on a.taxonomy_id=b.taxonomy_id " \
                    f"where a.project_accession='{self.project_accession}';"
            rows = get_all_results_for_query(conn, query)
        if len(rows) < 1:
            raise ValueError(f'No taxonomy for {self.project_accession} found in metadata DB.')
        elif len(rows) > 1:
            raise ValueError(f'Multiple taxonomy for {self.project_accession} found in metadata DB.')
        tax_id, sci_name = rows[0]
        self.eload_cfg.set('submission', 'taxonomy_id', value=tax_id)
        self.eload_cfg.set('submission', 'scientific_name', value=sci_name)

        with self.metadata_connection_handle as conn:
            query = f"select distinct analysis_accession, vcf_reference_accession " \
                    f"from analysis " \
                    f"where analysis_accession in {list_to_sql_in_list(self.analysis_accessions)};"
            rows = get_all_results_for_query(conn, query)
        for analysis_accession, asm_accession in rows:
            if not asm_accession:
                raise ValueError(f'No reference accession for {analysis_accession} found in metadata DB.')
            self.eload_cfg.set('submission', 'analyses', analysis_accession, 'assembly_accession', value=asm_accession)
            fasta_path, report_path = get_reference_fasta_and_report(sci_name, asm_accession)
            self.eload_cfg.set('submission', 'analyses', analysis_accession, 'assembly_fasta', value=fasta_path)
            self.eload_cfg.set('submission', 'analyses', analysis_accession, 'assembly_report', value=report_path)

    def find_local_file(self, fn):
        full_path = os.path.join(self._get_dir('vcf'), fn)
        if not os.path.exists(full_path):
            self.warning(f'File not found: {full_path}')
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

    def find_file_on_ena(self, fn, analysis):
        basename = os.path.basename(fn)
        full_path = os.path.join(self._get_dir('vcf'), basename)
        if not os.path.exists(full_path):
            try:
                self.info(f'Retrieve {basename} in {analysis} from ENA ftp')
                ftp_urls = self._get_files_from_ena_analysis(analysis)
                urls = [ftp_url for ftp_url in ftp_urls if ftp_url.endswith(fn)]
                if len(urls) == 1:
                    url = urls[0].replace('ftp', 'https')
                    download_file(url, full_path)
                else:
                    self.error(f'Could find {fn} in analysis {analysis} on ENA: most likely does not exist')
                    raise FileNotFoundError(f'File not found: {full_path}')
            except urllib.error.URLError:
                self.error(f'Could not access {url} on ENA: most likely does not exist')
                raise FileNotFoundError(f'File not found: {full_path}')
        return full_path

    def get_analysis_info(self):
        """Adds analysis info into the config: analysis accession(s), and vcf and index files."""
        with self.metadata_connection_handle as conn:
            query = f"select a.analysis_accession, array_agg(c.filename) " \
                    f"from analysis a " \
                    f"join analysis_file b on a.analysis_accession=b.analysis_accession " \
                    f"join file c on b.file_id=c.file_id " \
                    f"where a.analysis_accession in {list_to_sql_in_list(self.analysis_accessions)}" \
                    f"group by a.analysis_accession;"
            rows = get_all_results_for_query(conn, query)

        for analysis_accession, filenames in rows:
            # Uses the analysis accession as analysis alias
            self.eload_cfg.set('brokering', 'ena', 'ANALYSIS', analysis_accession, value=analysis_accession)
            vcf_file_list = []
            for fn in filenames:
                if not fn.endswith('.vcf.gz'):
                    self.warning(f'Ignoring {fn} because it is not a VCF')
                    continue
                try:
                    full_path = self.find_local_file(fn)
                except FileNotFoundError:
                    full_path = self.find_file_on_ena(fn, analysis_accession)
                vcf_file_list.append(full_path)

            # Using analysis_accession instead of analysis alias. This should not have any detrimental effect on
            # ingestion
            self.eload_cfg.set('submission', 'analyses', analysis_accession, 'vcf_files', value=vcf_file_list)

    def _analysis_report(self, all_analysis):
        reports = []
        for analysis_accession in all_analysis:
            assembly = all_analysis.get(analysis_accession).get('assembly_accession', '')
            fasta = all_analysis.get(analysis_accession).get('assembly_fasta', '')
            vcf_files_str = '\n'.join(all_analysis.get(analysis_accession).get('vcf_files', []))
            reports.append(f"""{analysis_accession}
  - Assembly: {assembly}
  - Fasta file: {fasta}
  - VCF file: 
{vcf_files_str}""")
        return '\n'.join(reports)

    def report(self):
        """Collect information from the config and write the report."""
        report_data = {
            'project': self.eload_cfg.query('brokering', 'ena', 'PROJECT', ret_default=''),
            'analyses': ', '.join(self.eload_cfg.query('brokering', 'ena', 'ANALYSIS', ret_default=[])),
            'analyses_report': self._analysis_report(self.eload_cfg.query('brokering', 'analyses', ret_default=[]))
        }

        report = """Results of backlog study preparation:
Project accession: {project}
Analysis accession(s): {analyses}
Analysis information: {analyses_report}
"""
        print(report.format(**report_data))
