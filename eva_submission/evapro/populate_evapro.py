import datetime
import os
import re
from functools import cached_property
from urllib.parse import urlsplit

from ebi_eva_common_pyutils.assembly_utils import is_patch_assembly
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.ena_utils import get_scientific_name_and_common_name
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.ncbi_utils import get_ncbi_assembly_name_from_term
from ebi_eva_internal_pyutils.config_utils import get_metadata_creds_for_profile
from ebi_eva_internal_pyutils.metadata_utils import build_taxonomy_code
from sqlalchemy import select, create_engine, func, update
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from eva_submission.evapro.find_from_ena import OracleEnaProjectFinder
from eva_submission.evapro.table import Project, Taxonomy, LinkedProject, Submission, ProjectEnaSubmission, \
    EvaSubmission, ProjectEvaSubmission, Analysis, AssemblySet, AccessionedAssembly, File, BrowsableFile, \
    Platform, ExperimentType, Sample, SampleInFile, ProjectSampleTemp1, ClusteredVariantUpdate
from eva_submission.samples_checker import get_samples_from_vcf

ena_ftp_file_prefix_path = "/ftp.sra.ebi.ac.uk/vol1"


def get_ftp_path(filename, analysis_accession_id):
    return f"{ena_ftp_file_prefix_path}/{analysis_accession_id[0:6]}/{analysis_accession_id}/{filename}"

def now():
    return datetime.datetime.now()

class EvaProjectLoader(AppLogger):
    """
    Class to insert project and its components into the metadata database

    loader = EvaProjectLoader()
    loader.load_project_from_ena(project_accession) -> Retrieve a project from ENA and add it to the metadata database
    loader.load_samples_from_vcf_file(sample_name_2_sample_accession, vcf_file, vcf_file_md5) -> Retrieve samples from a VCF file
    loader.load_samples_from_analysis(sample_name_2_sample_accession, analysis_accession) -> Retrieve samples for all files in an analysis
    The last 2 methods assume the project/analysis and file have been loaded already
    """

    def __init__(self):
        self.ena_project_finder = OracleEnaProjectFinder()

    def load_project_from_ena(self, project_accession, eload, analysis_accession_to_load=None):
        """
        Loads a project from ENA for the given ELOAD and adds it to the metadata database.
        If analysis_accession_to_load is specified, will only load that analysis; otherwise all analyses are added.
        """
        self.begin_or_continue_transaction()

        ###
        # LOAD PROJECT
        ###
        (
            study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
            project_title, taxonomy_id, scientific_name, common_name, study_description
        ) = self.ena_project_finder.find_project_from_ena_database(project_accession)

        project_obj = self.insert_project_in_evapro(
            project_accession=project_accession, center_name=center_name, project_alias=project_alias,
            title=project_title, description=study_description, ena_study_type=study_type,
            ena_secondary_study_id=study_id
        )

        ###
        # LOAD PARENT PROJECT
        ###
        parent_projects = self.ena_project_finder.find_parent_projects(project_accession=project_accession)
        for parent_project in parent_projects:
            self.insert_linked_projects(project_obj, parent_project)

        ###
        # LOAD TAXONOMY
        ###
        taxonomy_obj = self.insert_taxonomy(taxonomy_id)
        if taxonomy_obj not in project_obj.taxonomies:
            project_obj.taxonomies.append(taxonomy_obj)

        ###
        # LOAD SUBMISSIONS
        ###
        for submission_info in self.ena_project_finder.find_ena_submission_for_project(
                project_accession=project_accession):
            submission_id, alias, last_updated, hold_date, action = submission_info
            # action {"type": ADD, "schema": project, "source": ELOAD.Project.xml}
            submission_obj = self.insert_ena_submission(ena_submission_accession=submission_id,
                                                        action=action.get('type'),
                                                        submission_alias=alias, submission_date=last_updated,
                                                        brokered=1,
                                                        submission_type=action.get('schema').upper() if action.get(
                                                            'schema') else 'PROJECT')
            self.insert_project_ena_submission(project_obj, submission_obj, eload)
            # TODO: Link analysis with submission
        ###
        # LOAD ANALYSIS
        ###
        for analysis_info in self.ena_project_finder.find_analysis_in_ena(project_accession=project_accession):
            (
                analysis_accession, analysis_title, analysis_alias, analysis_description, analysis_type, center_name,
                first_created, assembly, refname, custom, experiment_types, platforms
            ) = analysis_info
            if analysis_accession_to_load and analysis_accession != analysis_accession_to_load:
                continue
            assembly_set_obj = self.insert_assembly_set(taxonomy_obj=taxonomy_obj, assembly_accession=assembly)
            analysis_obj = self.insert_analysis(
                analysis_accession=analysis_accession, title=analysis_title, alias=analysis_alias,
                description=analysis_description, center_name=center_name, date=first_created,
                assembly_set_id=assembly_set_obj.assembly_set_id, vcf_reference_accession=assembly)
            if analysis_obj not in project_obj.analyses:
                project_obj.analyses.append(analysis_obj)

            ###
            # LOAD SUBMISSIONS FOR ANALYSIS
            ###
            # This will likely retrieve the same submission as the one associated with the study but there might be
            # some specific to an analysis when they are submitted separately
            for submission_info in self.ena_project_finder.find_ena_submission_for_analysis(
                    analysis_accession=analysis_accession):
                submission_id, alias, last_updated, hold_date, action = submission_info
                submission_obj = self.insert_ena_submission(ena_submission_accession=submission_id,
                                                            action=action.get('type'),
                                                            submission_alias=alias, submission_date=last_updated,
                                                            brokered=1,
                                                            submission_type=action.get('schema').upper())
                if submission_obj not in analysis_obj.submissions:
                    analysis_obj.submissions.append(submission_obj)
            ###
            # LINK PLATFORMS
            ###
            platform_objs = []
            for platform in platforms:
                plat_obj = self.get_platform_obj_from_evapro(platform)
                # Bypass new platforms
                # TODO this mimics the behaviour of the perl script, but we should update
                if plat_obj:
                    platform_objs.append(plat_obj)
            analysis_obj.platforms = platform_objs

            ###
            # LOAD EXPERIMENT TYPE
            ###
            experiment_type_objs = [self.insert_experiment_type(experiment_type) for experiment_type in
                                    experiment_types]
            analysis_obj.experiment_types = experiment_type_objs

            ###
            # LOAD FILE
            ###
            for file_info in self.ena_project_finder.find_files_in_ena(analysis_accession=analysis_accession):
                analysis_accession, submission_file_id, filename, file_md5, file_type, status_id = file_info
                ftp_file = get_ftp_path(filename=filename, analysis_accession_id=analysis_accession)
                file_obj = self.insert_file(
                    project_accession=project_accession,
                    assembly_set_id=assembly_set_obj.assembly_set_id,
                    ena_submission_file_id=submission_file_id,
                    filename=filename,
                    file_md5=file_md5,
                    file_type=file_type,
                    ftp_file=ftp_file
                )
                if file_obj not in analysis_obj.files:
                    analysis_obj.files.append(file_obj)

            ###
            # LOAD SAMPLE
            ###
            for sample_info in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                sample_id, sample_accession = sample_info
                sample_obj = self.insert_sample(biosample_accession=sample_accession, ena_accession=sample_id)
        self.eva_session.commit()
        self.eva_session.close()


    def load_samples_from_vcf_file(self, sample_name_2_sample_accession, vcf_file, vcf_file_md5, sample_mapping = None):
        """sample_mapping is a dict"""
        sample_names = get_samples_from_vcf(vcf_file)
        self.begin_or_continue_transaction()
        file_obj = self.get_file(vcf_file_md5)
        if not file_obj:
            self.error(f'Cannot find file {vcf_file} in EVAPRO for md5 {vcf_file_md5}: Rolling back')
            return False
        for sample_name in sample_names:
            sample_accession = sample_name_2_sample_accession.get(sample_name)
            if not sample_accession and sample_mapping:
                mapping = sample_mapping.get(sample_name)
                if 'sample_name' in mapping:
                    sample_name = mapping['sample_name']
                sample_accession = sample_name_2_sample_accession.get(sample_name)
                if 'biosample_accession' in mapping:
                    sample_accession = mapping['biosample_accession']
                sample_name = sample_mapping.get(sample_name) or sample_name
            if not sample_accession:
                self.error(f'Sample {sample_name} found in {vcf_file} does not have BioSample accession: Rolling back')
                self.eva_session.rollback()
                return False
            sample_obj = self.get_sample(sample_accession)
            if not sample_obj:
                self.error(f'Cannot find sample {sample_accession} ({sample_name}) from {vcf_file} in EVAPRO: Rolling back')
                self.eva_session.rollback()
                return False
            self.insert_sample_in_file(file_id=file_obj.file_id, sample_id=sample_obj.sample_id,
                                       name_in_file=sample_name)
        self.eva_session.commit()
        return True

    def load_samples_from_analysis(self, sample_name_2_sample_accession, analysis_accession):
        # For analyses with aggregated VCFs, get sample accessions associated with the analysis
        sample_accessions = [sample_info[1] for sample_info in
                             self.ena_project_finder.find_samples_in_ena(analysis_accession)]
        self.info(f'Sample accessions for {analysis_accession} found in ENA: {sample_accessions}')
        sample_accession_2_sample_name = dict(
            zip(sample_name_2_sample_accession.values(), sample_name_2_sample_accession.keys()))
        self.begin_or_continue_transaction()
        file_objs = self.get_files_for_analysis(analysis_accession)
        for sample_accession in sample_accessions:
            sample_name = sample_accession_2_sample_name.get(sample_accession)
            sample_obj = self.get_sample(sample_accession)
            if not sample_obj:
                self.error(f'Cannot find sample {sample_accession} in EVAPRO')
                self.eva_session.rollback()
                return False
            # Associate these samples with all files in the analysis
            for file_obj in file_objs:
                self.insert_sample_in_file(file_id=file_obj.file_id, sample_id=sample_obj.sample_id,
                                           name_in_file=sample_name)
        self.eva_session.commit()
        return True

    def update_project_samples_temp1(self, project_accession):
        # This function assumes that all samples have been loaded to Sample/SampleFiles
        # TODO: Remove this when Sample have been back-filled and this can be calculated on the fly
        self.begin_or_continue_transaction()
        query = (
            select(Sample.biosample_accession).distinct()
            .join(SampleInFile, Sample.files)
            .join(File, SampleInFile.file)
            .join(Analysis, File.analyses)
            .join(Project, Analysis.projects)
            .where(Project.project_accession == project_accession)
        )
        result = self.eva_session.execute(query).fetchall()
        nb_samples = len(result)

        query = select(ProjectSampleTemp1).where(ProjectSampleTemp1.project_accession == project_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            project_samples_temp_obj = result.ProjectSampleTemp1
        else:
            project_samples_temp_obj = ProjectSampleTemp1(project_accession=project_accession)
            self.eva_session.add(project_samples_temp_obj)
        project_samples_temp_obj.sample_count = nb_samples
        self.eva_session.commit()

    def insert_browsable_files_for_project(self, project_accession):
        # insert into browsable file table, if files not already there
        query_browsable_files = select(BrowsableFile).where(BrowsableFile.project_accession == project_accession)
        query_files_to_be_browsable = (
            select(File)
            .join(File.analyses)
            .join(Analysis.projects)
            .where(Project.project_accession == project_accession, File.file_type.ilike('vcf'))
        )
        self.begin_or_continue_transaction()

        browsable_file_objs = self.eva_session.execute(query_browsable_files).scalars().all()
        file_to_be_browsable_objs = self.eva_session.execute(query_files_to_be_browsable).scalars().all()

        if len(browsable_file_objs) > 0:
            if set([f.filename for f in browsable_file_objs]) == set([f.filename for f in file_to_be_browsable_objs]):
                self.info('Browsable files already inserted, skipping')
            else:
                self.warning(f'Found {len(browsable_file_objs)} browsable file rows in the table but they are different '
                             f'from the expected ones: '
                             f'{os.linesep + os.linesep.join([str(obj) for obj in file_to_be_browsable_objs])}')
        else:
            self.info('Inserting browsable files...')
            for file_to_be_browsable_obj in file_to_be_browsable_objs:
                browsable_file_obj = BrowsableFile(
                    file_id=file_to_be_browsable_obj.file_id,
                    ena_submission_file_id=file_to_be_browsable_obj.ena_submission_file_id,
                    filename=file_to_be_browsable_obj.filename,
                    assembly_set_id= file_to_be_browsable_obj.analyses[0].assembly_set_id,
                    project_accession=project_accession
                )
                self.info(f'Add Browsable File {browsable_file_obj.filename} to EVAPRO')
                self.eva_session.add(browsable_file_obj)
            self.eva_session.commit()

    def mark_release_browsable_files_for_project(self, project_accession, release_date):
        self.begin_or_continue_transaction()
        update_browsable_files = (update(BrowsableFile)
                                 .where(BrowsableFile.project_accession == project_accession)
                                 .values(loaded=True, eva_release = f"{release_date.strftime('%Y%m%d')}"))
        self.eva_session.execute(update_browsable_files)
        self.eva_session.commit()

    def update_files_with_ftp_path_for_project(self, project_accession):
        query_browsable_files = select(BrowsableFile).where(BrowsableFile.project_accession == project_accession)
        self.begin_or_continue_transaction()
        for browsable_file_obj in self.eva_session.execute(query_browsable_files).scalars():
            file_obj = browsable_file_obj.file
            file_obj.ftp_file = f'/ftp.ebi.ac.uk/pub/databases/eva/{project_accession}/{file_obj.filename}'
            self.eva_session.add(file_obj)
        self.eva_session.commit()

    def update_loaded_assembly_in_browsable_files_for_project(self, project_accession):
        query_browsable_files = select(BrowsableFile).where(BrowsableFile.project_accession == project_accession)
        self.begin_or_continue_transaction()
        for browsable_file_obj in self.eva_session.execute(query_browsable_files).scalars():
            if browsable_file_obj.loaded_assembly is None:
                browsable_file_obj.loaded_assembly = browsable_file_obj.file.analyses[0].vcf_reference_accession
                self.eva_session.add(browsable_file_obj)
        self.eva_session.commit()

    def refresh_study_browser(self):
        self.begin_or_continue_transaction()
        self.eva_session.execute('REFRESH MATERIALIZED VIEW study_browser')

    def load_clustering_record(self, taxonomy, assembly, clustering_source):
        self.begin_or_continue_transaction()
        query = select(ClusteredVariantUpdate).where(ClusteredVariantUpdate.taxonomy_id==taxonomy,
                                                   ClusteredVariantUpdate.assembly_accession==assembly,
                                                   ClusteredVariantUpdate.source==clustering_source)
        clustering_record_obj = self.eva_session.execute(query).scalar()
        if not clustering_record_obj:
            clustering_record_obj = ClusteredVariantUpdate(taxonomy_id=taxonomy, assembly_accession=assembly,source=clustering_source)
        clustering_record_obj.ingestion_time = now()
        self.eva_session.add(clustering_record_obj)
        self.eva_session.commit()

    def _evapro_engine(self):
        pg_url, pg_user, pg_pass = get_metadata_creds_for_profile(cfg['maven']['environment'],
                                                                  cfg['maven']['settings_file'])
        dbtype, host_url, port_and_db = urlsplit(pg_url).path.split(':')
        port, db = port_and_db.split('/')
        return create_engine(URL.create(
            'postgresql+psycopg2',
            username=pg_user,
            password=pg_pass,
            host=host_url.split('/')[-1],
            database=db,
            port=int(port)
        ))

    @cached_property
    def eva_session(self):
        session = Session(self._evapro_engine())
        return session

    def begin_or_continue_transaction(self):
        if not self.eva_session.is_active:
            self.eva_session.begin()

    def get_assembly_code_from_evapro(self, assembly):
        query = select(AssemblySet.assembly_code) \
            .join(AccessionedAssembly, AssemblySet.assembly_set_id == AccessionedAssembly.assembly_set_id) \
            .where(AccessionedAssembly.assembly_accession == assembly)
        rows = set(self.eva_session.execute(query).fetchall())
        if len(rows) == 0:
            return None
        elif len(rows) > 1:
            options = ', '.join([row for row, in rows])
            raise ValueError(f'More than one possible code for assembly {assembly} found: {options}')
        return rows[0][0]

    def get_assembly_code(self, assembly, ncbi_api_key=None):
        # TODO: deduplicate the code taken from ebi_eva_internal_pyutils.metadata_utils.py
        assembly_code = self.get_assembly_code_from_evapro(assembly)
        if not assembly_code:
            assembly_name = get_ncbi_assembly_name_from_term(assembly, api_key=ncbi_api_key)
            # If the assembly is a patch assembly ex: GRCh37.p8, drop the trailing patch i.e., just return grch37
            if is_patch_assembly(assembly):
                assembly_name = re.sub('\\.p[0-9]+$', '', assembly_name.lower())
            assembly_code = re.sub('[^0-9a-zA-Z]+', '', assembly_name.lower())
        return assembly_code

    def get_platform_obj_from_evapro(self, platform):
        query = select(Platform).where(Platform.platform == platform)
        result = self.eva_session.execute(query).fetchone()
        if result:
            platform_obj = result.Platform
            return platform_obj
        return None

    def insert_project_in_evapro(self, project_accession, center_name, project_alias, title, description,
                                 ena_study_type, ena_secondary_study_id, scope='multi-isolate', material='DNA',
                                 study_type='Control Set'):
        query = select(Project).where(Project.project_accession == project_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            project_obj = result.Project
        else:
            query = select(func.max(Project.eva_study_accession))
            result = self.eva_session.execute(query).fetchone()
            eva_study_accession = result[0]
            if not eva_study_accession:
                eva_study_accession = 1
            else:
                eva_study_accession += 1
            project_obj = Project(
                project_accession=project_accession, center_name=center_name, alias=project_alias, title=title,
                description=description, scope=scope, material=material, type=ena_study_type, study_type=study_type,
                secondary_study_id=ena_secondary_study_id, eva_study_accession=eva_study_accession
            )
            self.eva_session.add(project_obj)
            self.info(f'Add Project {project_accession} to EVAPRO')
        return project_obj

    def insert_publications_in_evapro(self):
        # TODO: This function was implemented in the perl script but never used.
        #  It might be useful to reimplement correctly
        pass

    def insert_taxonomy(self, taxonomy_id, eva_species_name=None):
        query = select(Taxonomy).where(Taxonomy.taxonomy_id == taxonomy_id)
        result = self.eva_session.execute(query).fetchone()
        if result:
            taxonomy_obj = result.Taxonomy
        else:
            scientific_name, common_name = get_scientific_name_and_common_name(taxonomy_id)
            taxonomy_code = build_taxonomy_code(scientific_name)
            # If a common name cannot be found then we should  use the scientific name
            eva_species_name = eva_species_name or common_name or scientific_name
            taxonomy_obj = Taxonomy(taxonomy_id=taxonomy_id, common_name=common_name, scientific_name=scientific_name,
                                    taxonomy_code=taxonomy_code, eva_name=eva_species_name)
            self.eva_session.add(taxonomy_obj)
            self.info(f'Add Taxonomy {taxonomy_id} to EVAPRO')
        return taxonomy_obj

    def insert_linked_projects(self, project_obj, linked_project_accession, project_relation='PARENT'):
        query = select(LinkedProject).where(LinkedProject.project_accession == project_obj.project_accession,
                                            LinkedProject.linked_project_accession == linked_project_accession,
                                            LinkedProject.linked_project_relation == project_relation)
        result = self.eva_session.execute(query).fetchone()
        if result:
            linked_project_obj = result.LinkedProject
        else:
            linked_project_obj = LinkedProject(project_accession=project_obj.project_accession,
                                               linked_project_accession=linked_project_accession,
                                               linked_project_relation=project_relation)
            self.eva_session.add(linked_project_obj)
            self.info(f'Add Project link ({project_obj.project_accession} -> {project_relation} -> '
                      f'{linked_project_accession})to EVAPRO')
        return linked_project_obj

    def insert_ena_submission(self, ena_submission_accession, action, submission_alias, submission_date, brokered=1,
                              submission_type='PROJECT'):
        query = select(Submission).where(Submission.submission_accession == ena_submission_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            submission_obj = result.Submission
        else:
            submission_obj = Submission(
                submission_accession=ena_submission_accession, action=action, title=submission_alias,
                date=submission_date, brokered=brokered, type=submission_type
            )
            self.eva_session.add(submission_obj)
            self.info(f'Add Submission {ena_submission_accession} {action} to EVAPRO')
        return submission_obj

    def insert_project_ena_submission(self, project_obj, submission_obj, eload):
        """
        This function links project and ENA submission and project and ELOAD in EVAPRO.
        TODO: This is project specific where it should be analysis specific.
        """
        query = select(ProjectEnaSubmission).where(
            ProjectEnaSubmission.project_accession == project_obj.project_accession,
            ProjectEnaSubmission.submission_id == submission_obj.submission_id
        )
        result = self.eva_session.execute(query).fetchone()
        if result:
            project_ena_submission_obj = result.ProjectEnaSubmission
        else:

            project_ena_submission_obj = ProjectEnaSubmission(project_accession=project_obj.project_accession,
                                                              submission_id=submission_obj.submission_id)
            self.eva_session.add(project_ena_submission_obj)
        query = select(EvaSubmission).where(EvaSubmission.eva_submission_id == eload)
        result = self.eva_session.execute(query).fetchone()
        if result:
            eva_submission_obj = result.EvaSubmission
        else:
            eva_submission_obj = EvaSubmission(eva_submission_id=eload, eva_submission_status_id=6)
            self.eva_session.add(eva_submission_obj)
        query = select(ProjectEvaSubmission).where(
            ProjectEvaSubmission.project_accession == project_obj.project_accession,
            ProjectEvaSubmission.eload_id == eload
        )
        result = self.eva_session.execute(query).fetchone()
        if result:
            project_eva_submission_obj = result.ProjectEvaSubmission
        else:
            project_eva_submission_obj = ProjectEvaSubmission(project_accession=project_obj.project_accession,
                                                              old_ticket_id=eload, eload_id=eload)
            self.eva_session.add(project_eva_submission_obj)

    def insert_analysis(self, analysis_accession, title, alias, description, center_name, date, assembly_set_id,
                        vcf_reference_accession=None):
        query = select(Analysis).where(Analysis.analysis_accession == analysis_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            analysis_obj = result.Analysis
        else:
            analysis_obj = Analysis(analysis_accession=analysis_accession, title=title, alias=alias,
                                    description=description,
                                    center_name=center_name, date=date, assembly_set_id=assembly_set_id)
            if vcf_reference_accession:
                analysis_obj.vcf_reference_accession = vcf_reference_accession
            self.eva_session.add(analysis_obj)
            self.info(f'Add Analysis {analysis_accession} to EVAPRO')
        return analysis_obj

    def insert_assembly_set(self, taxonomy_obj, assembly_accession, assembly_name=None):
        if not assembly_name:
            assembly_name = get_ncbi_assembly_name_from_term(assembly_accession, api_key=cfg.get('eutils_api_key'))

        query = select(AssemblySet).where(AssemblySet.taxonomy_id == taxonomy_obj.taxonomy_id,
                                          AssemblySet.assembly_name == assembly_name)
        result = self.eva_session.execute(query).fetchone()
        if result:
            assembly_set_obj = result.AssemblySet
        else:
            assembly_code = self.get_assembly_code(assembly_accession, ncbi_api_key=cfg.get('eutils_api_key'))
            assembly_set_obj = AssemblySet(taxonomy_id=taxonomy_obj.taxonomy_id, assembly_name=assembly_name,
                                           assembly_code=assembly_code)
            self.eva_session.add(assembly_set_obj)
            self.info(f'Add assembly_set ({taxonomy_obj.taxonomy_id} {assembly_name}) to EVAPRO')
            self.eva_session.flush()

            accessioned_assembly_obj = AccessionedAssembly(
                assembly_set_id=assembly_set_obj.assembly_set_id, assembly_accession=assembly_accession,
                assembly_chain=assembly_accession.split('.')[0], assembly_version=assembly_accession.split('.')[1]
            )
            self.eva_session.add(accessioned_assembly_obj)
            self.info(f'Add accessioned_assembly {assembly_accession} to EVAPRO')

        return assembly_set_obj

    def insert_custom_assembly_set(self, taxonomy_obj, custom_assembly):
        # TODO: Check if we still need to support this.
        pass

    def get_file(self, file_md5):
        query = select(File).where(File.file_md5 == file_md5)
        result = self.eva_session.execute(query).fetchone()
        if result:
            return result.File
        return None

    def get_files_for_analysis(self, analysis_accession):
        query = select(File).join(Analysis, File.analyses).where(Analysis.analysis_accession == analysis_accession)
        return [result.File for result in self.eva_session.execute(query).fetchall()]

    def insert_file(self, project_accession, assembly_set_id, ena_submission_file_id, filename, file_md5, file_type,
                    ftp_file, file_location=None, file_class='submitted', file_version=1, is_current=1):
        file_obj = self.get_file(file_md5)
        if not file_obj:
            file_obj = File(
                ena_submission_file_id=ena_submission_file_id, filename=filename, file_md5=file_md5,
                file_type=file_type,
                file_location=file_location, file_class=file_class, file_version=file_version, is_current=is_current,
                ftp_file=ftp_file
            )
            self.eva_session.add(file_obj)
            self.info(f'Add File {filename} {file_md5} to EVAPRO')

        if file_type.lower() in {'vcf', 'vcf_aggregate'}:
            query = select(BrowsableFile).where(BrowsableFile.file_id == file_obj.file_id)
            result = self.eva_session.execute(query).fetchone()
            if result:
                browsable_file_obj = result.BrowsableFile
            else:
                browsable_file_obj = BrowsableFile(
                    file_id=file_obj.file_id, ena_submission_file_id=ena_submission_file_id, filename=filename,
                    project_accession=project_accession, assembly_set_id=assembly_set_id
                )
                self.eva_session.add(browsable_file_obj)
                self.info(f'Add Browsable file {filename} to EVAPRO')

        return file_obj

    def insert_experiment_type(self, experiment_type):
        query = select(ExperimentType).where(ExperimentType.experiment_type == experiment_type)
        result = self.eva_session.execute(query).fetchone()
        if result:
            experiment_type_obj = result.ExperimentType
        else:
            experiment_type_obj = ExperimentType(experiment_type=experiment_type)
            self.eva_session.add(experiment_type_obj)
            self.info(f'Add Experiment type {experiment_type} to EVAPRO')

        return experiment_type_obj

    def get_sample(self, biosample_accession):
        query = select(Sample).where(Sample.biosample_accession == biosample_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            return result.Sample
        return None

    def insert_sample(self, biosample_accession, ena_accession):
        sample_obj = self.get_sample(biosample_accession)
        if not sample_obj:
            sample_obj = Sample(biosample_accession=biosample_accession, ena_accession=ena_accession)
            self.eva_session.add(sample_obj)
            self.info(f'Add Sample {biosample_accession} to EVAPRO')
        else:
            self.debug(f'Sample {biosample_accession} already exists in EVAPRO')
        return sample_obj

    def insert_sample_in_file(self, file_id, sample_id, name_in_file):
        query = select(SampleInFile).where(SampleInFile.file_id == file_id, SampleInFile.sample_id == sample_id)
        result = self.eva_session.execute(query).fetchone()
        if result:
            sample_in_file_obj = result.SampleInFile
        else:
            sample_in_file_obj = SampleInFile(file_id=file_id, sample_id=sample_id, name_in_file=name_in_file)
            self.eva_session.add(sample_in_file_obj)
            self.info(f'Add SampleInFile {file_id} and {sample_id} to EVAPRO')
        return sample_in_file_obj
