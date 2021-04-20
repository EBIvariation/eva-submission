import os
from xml.etree import ElementTree as ET

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
import requests
from requests.auth import HTTPBasicAuth

from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import get_metadata_conn, get_genome_fasta_and_report


class EloadBacklog(Eload):

    def fill_in_config(self):
        """Fills in config params from metadata DB and ENA, enabling later parts of pipeline to run."""
        self.eload_cfg.set('brokering', 'ena', 'PROJECT', value=self.project_accession)
        self.get_species_info()
        self.get_analysis_info()
        self.get_hold_date()
        self.eload_cfg.write()

    @cached_property
    def project_accession(self):
        with get_metadata_conn() as conn:
            query = f"select project_accession from evapro.project_eva_submission where eload_id={self.eload_num};"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            raise ValueError(f'No project accession for {self.eload} found in metadata DB.')
        return rows[0][0]

    @cached_property
    def project_alias(self):
        with get_metadata_conn() as conn:
            query = f"select alias from evapro.project where project_accession={self.project_accession};"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            raise ValueError(f'No project alias for {self.project_accession} found in metadata DB.')
        return rows[0][0]

    def get_species_info(self):
        """Adds species info into the config: taxonomy id and scientific name,
        and assembly accession, fasta, and report."""
        with get_metadata_conn() as conn:
            # TODO note this will get assembly info based on the taxonomy
            query = f"select a.taxonomy_id, b.scientific_name, d.assembly_accession " \
                    f"from project_taxonomy a " \
                    f"join taxonomy b on a.taxonomy_id=b.taxonomy_id " \
                    f"join assembly_set c on b.taxonomy_id=c.taxonomy_id " \
                    f"join accessioned_assembly d on c.assembly_set_id=d.assembly_set_id " \
                    f"where a.project_accession='{self.project_accession}';"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            raise ValueError(f'No taxonomy for {self.project_accession} found in metadata DB.')
        tax_id, sci_name, asm_accession = rows[0]
        self.eload_cfg.set('submission', 'taxonomy_id', value=tax_id)
        self.eload_cfg.set('submission', 'scientific_name', value=sci_name)
        self.eload_cfg.set('submission', 'assembly_accession', value=asm_accession)

        fasta_path, report_path = get_genome_fasta_and_report(sci_name, asm_accession)
        self.eload_cfg.set('submission', 'assembly_fasta', value=fasta_path)
        self.eload_cfg.set('submission', 'assembly_report', value=report_path)

    def get_analysis_info(self):
        """Adds analysis info into the config: analysis accession(s), and vcf and index files."""
        with get_metadata_conn() as conn:
            query = f"select a.analysis_accession, array_agg(c.filename) " \
                    f"from project_analysis a " \
                    f"join analysis_file b on a.analysis_accession=b.analysis_accession " \
                    f"join file c on b.file_id=c.file_id " \
                    f"where a.project_accession='{self.project_accession}' " \
                    f"group by a.analysis_accession;"
            rows = get_all_results_for_query(conn, query)
        if len(rows) == 1:
            raise ValueError(f'No analyses for {self.project_accession} found in metadata DB.')

        for analysis_accession, filenames in rows:
            # TODO for now we assume a single analysis per project as that's what the eload config supports
            self.eload_cfg.set('brokering', 'ena', 'ANALYSIS', value=analysis_accession)
            for fn in filenames:
                full_path = os.path.join(self._get_dir('vcf'), fn)
                if not os.path.exists(full_path):
                    raise ValueError(f'File not found: {full_path}')
                if full_path.endswith('tbi'):
                    index_file = full_path
                else:
                    vcf_file = full_path
            if not index_file or not vcf_file:
                raise ValueError(f'VCF file or index file is missing for analysis {analysis_accession}')
            # TODO is it necessary that brokering and submission vcfs be different paths?
            self.eload_cfg.set('submission', 'vcf_files', vcf_file, 'index', value=index_file)
            self.eload_cfg.set('brokering', 'vcf_files', vcf_file, 'index', value=index_file)

    def get_hold_date(self):
        """Gets hold date from ENA and adds to the config."""
        xml_request = f"""<SUBMISSION_SET>
            <SUBMISSION>
                <ACTIONS>
                    <ACTION>
                        <RECEIPT target="{self.project_alias}"/>
                   </ACTION>
               </ACTIONS>
            </SUBMISSION>
        </SUBMISSION_SET>"""
        response = requests.post(
            cfg.query('ena', 'submit_url'),
            auth=HTTPBasicAuth(cfg.query('ena', 'username'), cfg.query('ena', 'password')),
            files={'SUBMISSION': xml_request}
        )
        receipt = ET.fromstring(response.text)
        try:
            project_elt = receipt.findall('PROJECT')[0]
            hold_date = project_elt.attrib['holdUntilDate']
        except (IndexError, KeyError):
            raise ValueError(f"Couldn't get hold date from ENA for {self.project_accession} ({self.project_alias})")
        self.eload_cfg.set('brokering', 'ena', 'hold_date', value=hold_date)
