import glob
import gzip
import os
import shutil
from functools import cached_property

import requests
from ebi_eva_common_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

from retry import retry

from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_contig_alias_db_creds_for_profile


from eva_submission.eload_submission import Eload, directory_structure
from eva_submission.eload_utils import resolve_accession_from_text, get_reference_fasta_and_report, NCBIAssembly
from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import EloadConfig
from eva_submission.submission_in_ftp import FtpDepositBox
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter


class EloadStatus(Eload):

    def __init__(self, eload_number: int, config_object: EloadConfig = None):
        self.eload_num = eload_number
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.config_path = os.path.join(self.eload_dir, '.' + self.eload + '_config.yml')
        if config_object:
            self.eload_cfg = config_object
        else:
            self.eload_cfg = EloadConfig(self.config_path)

    @cached_property
    def tmp_dir(self):
        tmp_dir = '.'
        os.makedirs(tmp_dir, exist_ok=True)
        return tmp_dir

    def retrieve_project_from_config(self):
        eload_config_file_name = '.' + self.eload + '_config.yml'
        if not self.eload_exists():
            eload_retrieval = ELOADRetrieval()
            eload_retrieval.retrieve_eloads_and_projects(self.eload,
                                                         eload_dirs_files=eload_config_file_name,
                                                         eload_retrieval_dir=self.tmp_dir)
            eload_cfg = EloadConfig(os.path.join(self.tmp_dir, eload_config_file_name))
        else:
            eload_cfg = EloadConfig(os.path.join(self.eload_dir, eload_config_file_name))
        return eload_cfg.query('brokering', 'ena', 'PROJECT')

    def retrieve_project_from_metadata(self):
        with self.metadata_connection_handle as conn:
            query = f"select project_accession from evapro.project_eva_submission where eload_id={self.eload_num};"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            self.info(f'No project accession for {self.eload} found in metadata DB.')
            return
        return rows[0][0]

    def status(self):
        print(self.project)

    def eload_exists(self):
        return os.path.exists(self.eload_dir)

    def project(self):
        project_accession = self.retrieve_project_from_config()
        if not project_accession:
            project_accession = self.retrieve_project_from_metadata()
        return project_accession

    @cached_property
    def mongo_conn(self):
        return get_mongo_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file'])

    def detect_project_status(self):
        for analysis, source_assembly, taxonomy, filenames in self.project_information():
            # initialise results with default values
            accessioning_status = remapping_status = clustering_status = target_assembly = 'Not found'
            list_ssid_accessioned, list_ssid_remapped, list_ssid_clustered = ([], [], [])
            if not taxonomy:
                self.error(f'No Assembly set present in the metadata for project: {self.project}:{analysis}')
                taxonomy = self.get_taxonomy_for_project()
            if taxonomy and taxonomy != 9606:
                list_ssid_accessioned = self.check_accessioning_was_done(analysis, filenames)
                accessioning_status = 'Done' if len(list_ssid_accessioned) > 0 else 'Pending'
                target_assembly = self.find_current_target_assembly_for(taxonomy)
                remapping_status = 'Required' if source_assembly != target_assembly else 'Not_required'
                if source_assembly != target_assembly:
                    list_ssid_remapped = self.check_remapping_was_done(target_assembly, list_ssid_accessioned)
                    if list_ssid_remapped:
                        remapping_status = 'Done'
                    assembly = target_assembly
                else:
                    assembly = source_assembly
                list_ssid_clustered = self.check_clustering_was_done(assembly, list_ssid_accessioned)
                clustering_status = 'Done' if list_ssid_clustered else 'Pending'
            else:
                if not taxonomy:
                    self.error(f'Project {self.project}:{analysis} has no taxonomy associated and the metadata '
                                 f'should be checked.')
            print('\t'.join((str(e) for e in [self.project, analysis, taxonomy, source_assembly, target_assembly,
                                              accessioning_status, len(list_ssid_accessioned), remapping_status,
                                              len(list_ssid_remapped), clustering_status,
                                              len(list_ssid_clustered)])))

    def project_information(self):
        """Retrieve project information from the metadata. Information retrieve include
        the analysis and associated taxonomy, genome and file names that are included in this project."""
        query = (
            "select distinct pa.project_accession, pa.analysis_accession, a.vcf_reference_accession, at.taxonomy_id, f.filename "
            "from project_analysis pa "
            "join analysis a on pa.analysis_accession=a.analysis_accession "
            "left join assembly_set at on at.assembly_set_id=a.assembly_set_id "
            "left join analysis_file af on af.analysis_accession=a.analysis_accession "
            "join file f on f.file_id=af.file_id "
            f"where f.file_type='VCF' and pa.project_accession='{self.project}'"
            "order by pa.project_accession, pa.analysis_accession")
        filenames = []
        current_analysis = current_assembly = current_tax_id = None
        for project, analysis, assembly, tax_id, filename in get_all_results_for_query(self.metadata_conn, query):
            if analysis != current_analysis:
                if current_analysis:
                    yield current_analysis, current_assembly, current_tax_id, filenames
                current_analysis = analysis
                current_assembly = assembly
                current_tax_id = tax_id
                filenames = []
            filenames.append(filename)
        yield current_analysis, current_assembly, current_tax_id, filenames

    def get_taxonomy_for_project(self):
        taxonomies = []
        query = f"select distinct taxonomy_id from evapro.project_taxonomy where project_accession='{self.project}'"
        for tax_id, in get_all_results_for_query(self.metadata_conn, query):
            taxonomies.append(tax_id)
        if len(taxonomies) == 1:
            return taxonomies[0]
        else:
            self.error(f'Cannot retrieve a single taxonomy for project {self.project}. Found {len(taxonomies)}.')

    def check_accessioning_was_done(self, analysis, filenames):
        """
        Check that an accessioning file can be found in either noah or codon (assume access to both filesystem)
        It parses and provide a 1000 submitted variant accessions from that project.
        """
        accessioning_reports = self.get_accession_reports_for_study()
        accessioned_filenames = [self.get_accession_file(f) for f in filenames]
        if not accessioning_reports:
            return []
        if len(accessioning_reports) == 1:
            accessioning_report = accessioning_reports[0]
        elif len([r for r in accessioning_reports if r in accessioned_filenames]) == 1:
            accessioning_report = [r for r in accessioning_reports if os.path.basename(r) in accessioned_filenames][
                0]
        elif len([r for r in accessioning_reports if analysis in r]) == 1:
            accessioning_report = [r for r in accessioning_reports if analysis in r][0]
        elif accessioning_reports:
            self.warning(
                f'Assume all accessioning reports are from project {self.project}:{analysis} and only use the first one.')
            accessioning_report = accessioning_reports[0]
        else:
            self.error(
                f'Cannot assign accessioning report to project {self.project} analysis {analysis} for files {accessioning_reports}')
            accessioning_report = None
        if not accessioning_report:
            return []
        return self.get_accessioning_info_from_file(accessioning_report)

    def get_accession_file(self, filename):
        basefile = ''
        if filename.endswith('.vcf.gz'):
            basefile = filename[:-7]
        elif filename.endswith('.vcf'):
            basefile = filename[:-4]
        return basefile + '.accessioned.vcf.gz '

    def get_accessioning_info_from_file(self, path):
        """
        Read the accessioning report to retrieve the first 1000 Submitted variant accessions
        """
        no_of_ss_ids_in_file = 0
        first_1000_ids = []
        with gzip.open(path, 'rt') as f:
            for line in f:
                if line.startswith('#'):
                    continue
                elif line.split("\t")[2].startswith("ss"):
                    no_of_ss_ids_in_file = no_of_ss_ids_in_file + 1
                    if no_of_ss_ids_in_file <= 1000:
                        first_1000_ids.append(int(line.split("\t")[2][2:]))
                    else:
                        break
        return first_1000_ids

    def get_accession_reports_for_study(self):
        """
        Given a study, find the accessioning report path for that study on both noah and codon.
        Look for files ending with accessioned.vcf.gz.
        """
        local_files = glob.glob(os.path.join(cfg['projects_dir'], self.project, '60_eva_public', '*accessioned.vcf.gz'))
        accessioning_reports = local_files
        if not accessioning_reports:
            self.error(f"Could not find any file in Noah or Codon for Study {self.project}")

        return accessioning_reports

    def check_remapping_was_done(self, target_assembly, list_ssid):
        ss_variants = self.find_submitted_variant_in_assembly(target_assembly, list_ssid)
        self.info(f'Found {len(ss_variants)} remapped variants out of {len(list_ssid)} in {target_assembly}')
        return [ss_variant['accession'] for ss_variant in ss_variants]

    def check_clustering_was_done(self, assembly, list_ssid):
        ss_variants = self.find_submitted_variant_in_assembly(assembly, list_ssid)
        ss_variants = [ss_variant['accession'] for ss_variant in ss_variants if 'rs' in ss_variant]
        self.info(f'Found {len(ss_variants)} clustered variants out of {len(list_ssid)} in {assembly}')
        return ss_variants

    def find_submitted_variant_in_assembly(self, assembly, list_ssid):
        filters = {'seq': assembly, 'accession': {'$in': list_ssid}}
        cursor = self.mongo_conn['eva_accession_sharded']['submittedVariantEntity'].find(filters)
        variants = []
        for variant in cursor:
            variants.append(variant)
        return variants

    def find_current_target_assembly_for(self, taxonomy):
        query = f"select assembly_id from evapro.supported_assembly_tracker where taxonomy_id={taxonomy} and current=true"
        assemblies = []
        for asm, in get_all_results_for_query(self.metadata_conn, query):
            assemblies.append(asm)
        assert len(assemblies) < 2, f'Multiple target assemblies found for taxonomy {taxonomy}'
        if assemblies:
            return assemblies[0]