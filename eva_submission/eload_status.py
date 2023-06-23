import csv
import glob
import gzip
import os
import subprocess
import sys
import tempfile
from functools import cached_property

from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.metadata_utils import resolve_variant_warehouse_db_name
from ebi_eva_common_pyutils.mongo_utils import get_mongo_connection_handle
from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query
from ebi_eva_common_pyutils.config import cfg
from eva_submission.eload_submission import Eload
from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import EloadConfig


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
        tmp_dir = tempfile.TemporaryDirectory()
        return tmp_dir.name

    @cached_property
    def eload_config_file(self):
        if not self.eload_dir_exists():
            eload_config_file = self.eload + '/.' + self.eload + '_config.yml'
            compressed_eload_config = eload_config_file + '.gz'
            try:
                eload_retrieval = ELOADRetrieval()
                eload_retrieval.retrieve_eloads_and_projects(
                    self.eload_num, retrieve_associated_project=False, update_path=False,
                    eload_dirs_files=[compressed_eload_config], project=None, project_dirs_files=None, eload_lts_dir=None,
                    project_lts_dir=None, eload_retrieval_dir=self.tmp_dir, project_retrieval_dir=None
                )
                eload_cfg = EloadConfig(os.path.join(self.tmp_dir, eload_config_file))
            except subprocess.CalledProcessError:
                return
        else:
            eload_cfg = EloadConfig(os.path.join(self.eload_dir, '.' + self.eload + '_config.yml'))
        return eload_cfg

    @cached_property
    def project_from_config(self):
        if self.eload_config_file:
            return self.eload_config_file.query('brokering', 'ena', 'PROJECT')

    @cached_property
    def analysis_from_config(self):
        if self.eload_config_file:
            return self.eload_config_file.query('brokering', 'ena', 'ANALYSIS')

    @cached_property
    def taxonomy_from_config(self):
        if self.eload_config_file:
            return self.eload_config_file.query('submission', 'taxonomy_id')

    def source_assembly_from_config(self, analysis):
        if self.eload_config_file:
            alias = [alias for alias, a in self.analysis_from_config.items() if analysis == a][0]
            return self.eload_config_file.query('submission', 'analyses', alias, 'assembly_accession')

    def retrieve_project_from_metadata(self):
        with self.metadata_connection_handle as conn:
            query = f"select project_accession from evapro.project_eva_submission where eload_id={self.eload_num};"
            rows = get_all_results_for_query(conn, query)
        if len(rows) != 1:
            self.info(f'No project accession for {self.eload} found in metadata DB.')
            return
        return rows[0][0]

    def eload_dir_exists(self):
        return os.path.exists(self.eload_dir)

    @cached_property
    def project(self):
        project_accession = self.project_from_config
        if not project_accession:
            project_accession = self.retrieve_project_from_metadata()
        return project_accession

    @cached_property
    def analyses(self):
        analysis_dict = self.analysis_from_config
        if analysis_dict:
            return analysis_dict.values()

    def status(self):
        header = [
            "eload", "project", "analysis", "taxonomy", "source_assembly", "target_assembly", "metadata_load_status",
            "accessioning_status", "remapping_status", "clustering_status", "variant_load_status",
            "statistics_status", "annotation_status"
        ]
        all_status = []
        if self.analyses:
            for analysis in self.analyses:
                status = self.status_per_analysis(analysis)
                if not status:
                    all_status.append({
                        "eload": self.eload,
                        "project": self.project,
                        "analysis": analysis,
                        "taxonomy": self.taxonomy_from_config,
                        "source_assembly": self.source_assembly_from_config(analysis),
                        "target_assembly": self.find_current_target_assembly_for(self.taxonomy_from_config),
                        "metadata_load_status": "Pending",
                        "accessioning_status": "Pending",
                        "remapping_status": "Pending",
                        "clustering_status": "Pending",
                        "variant_load_status": "Pending",
                        "statistics_status": "Pending",
                        "annotation_status": "Pending"
                    })
                else:
                    all_status.extend(status)
        else:
            all_status = self.status_per_analysis()
        writer = csv.DictWriter(sys.stdout, fieldnames=header)
        writer.writeheader()
        for st in all_status:
            writer.writerow(st)

    @cached_property
    def mongo_conn(self):
        return get_mongo_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file'])

    def status_per_analysis(self, analysis=None):
        st_per_analysis = []
        for analysis, source_assembly, taxonomy, filenames in self.project_information(analysis):
            # initialise results with default values
            accessioning_status = remapping_status = clustering_status = target_assembly = 'Not found'
            list_ssid_accessioned, list_ssid_remapped, list_ssid_clustered = ([], [], [])
            if not taxonomy:
                self.error(f'No Assembly set present in the metadata for project: {self.project}:{analysis}')
                taxonomy = self.get_taxonomy_for_project()
            if not taxonomy:
                self.error(f'Project {self.project}:{analysis} has no taxonomy associated and the metadata '
                           f'should be checked.')
                return

            if taxonomy != 9606:
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

            study_loaded, statistics_loaded, annotation_loaded = self.find_loaded_study_in_variant_warehouse(source_assembly, taxonomy, analysis)
            variant_load_status = 'Done' if study_loaded else 'Pending'
            statistics_status = 'Done' if statistics_loaded else 'Pending'
            annotation_status = 'Done' if annotation_loaded else 'Pending'
            st_per_analysis.append({
                "eload": self.eload,
                "project": self.project,
                "analysis": analysis,
                "taxonomy": taxonomy,
                "source_assembly": source_assembly,
                "target_assembly": target_assembly,
                "metadata_load_status": "Done",
                "accessioning_status": accessioning_status,
                "remapping_status": remapping_status,
                "clustering_status": clustering_status,
                "variant_load_status": variant_load_status,
                "statistics_status": statistics_status,
                "annotation_status": annotation_status
            })
        return st_per_analysis

    def project_information(self, analysis):
        """Retrieve project information from the metadata. Information retrieve include
        the analysis and associated taxonomy, genome and file names that are included in this project."""
        query = (
            "select distinct pa.project_accession, pa.analysis_accession, a.vcf_reference_accession, at.taxonomy_id, f.filename "
            "from project_analysis pa "
            "join analysis a on pa.analysis_accession=a.analysis_accession "
            "left join assembly_set at on at.assembly_set_id=a.assembly_set_id "
            "left join analysis_file af on af.analysis_accession=a.analysis_accession "
            "join file f on f.file_id=af.file_id "
            f"where f.file_type='VCF' and pa.project_accession='{self.project}' "
        )
        if analysis:
            query += f"and pa.analysis_accession='{analysis}'"
        else:
            query += "order by pa.project_accession, pa.analysis_accession"
        filenames = []
        current_analysis = current_assembly = current_tax_id = None
        with self.metadata_connection_handle as conn:
            for project, analysis, assembly, tax_id, filename in get_all_results_for_query(conn, query):
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
        with self.metadata_connection_handle as conn:
            for tax_id, in get_all_results_for_query(conn, query):
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
        ftp_files = glob.glob(os.path.join(cfg['public_ftp_dir'], self.project, '*accessioned.vcf.gz'))
        accessioning_reports = local_files + ftp_files
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

    def find_loaded_study_in_variant_warehouse(self, assembly, taxonomy, analysis):
        with self.metadata_connection_handle as conn:
            db_name = resolve_variant_warehouse_db_name(conn, assembly, taxonomy)
        filters = {'sid': self.project, 'fid': analysis}
        cursor = self.mongo_conn[db_name]['files_2_0'].find(filters)
        studies = list(cursor)
        study_loaded = statistics_loaded = annotation_loaded = False
        if len(studies) > 0:
            study_loaded = True
            if 'st' in studies[0]:
                statistics_loaded = True
        self.info('study_loaded ' + str(study_loaded))
        self.info('statistics_loaded ' + str(statistics_loaded))
        filters = {"files.sid": self.project, "files.fid": analysis}
        variant = self.mongo_conn[db_name]['variants_2_0'].find_one(filters)
        if variant:
            filters = {"_id": {"$regex": variant["_id"] + ".*"}}
            annotation = self.mongo_conn[db_name]['annotations_2_0'].find_one(filters)
            if annotation:
                annotation_loaded = True
        self.info('annotation_loaded ' + str(annotation_loaded))
        return study_loaded, statistics_loaded, annotation_loaded

    def find_current_target_assembly_for(self, taxonomy):
        query = f"select assembly_id from evapro.supported_assembly_tracker where taxonomy_id={taxonomy} and current=true"
        assemblies = []
        with self.metadata_connection_handle as conn:
            for asm, in get_all_results_for_query(conn, query):
                assemblies.append(asm)
        assert len(assemblies) < 2, f'Multiple target assemblies found for taxonomy {taxonomy}'
        if assemblies:
            return assemblies[0]
