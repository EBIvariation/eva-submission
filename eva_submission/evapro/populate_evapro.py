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
from requests import session
from sqlalchemy import select, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from eva_submission.evapro.find_from_ena import EnaProjectFinder
from eva_submission.evapro.table import Project, Taxonomy, LinkedProject, Submission, ProjectEnaSubmission, \
    EvaSubmission, ProjectEvaSubmission, Analysis, AssemblySet, AccessionedAssembly, File, BrowsableFile, \
    Platform, ExperimentType, Sample, SampleInFile
from eva_submission.samples_checker import get_samples_from_vcf

ena_ftp_file_prefix_path = "/ftp.sra.ebi.ac.uk/vol1"

def get_ftp_path(filename, accession_id):
    return f"{ena_ftp_file_prefix_path}/{accession_id[0:6]}/{accession_id}/{filename}"


class EvaProjectLoader(AppLogger):

    def __init__(self):
        self.ena_project_finder = EnaProjectFinder()

    def _evapro_engine(self):
        pg_url, pg_user, pg_pass = get_metadata_creds_for_profile(cfg['maven']['environment'], cfg['maven']['settings_file'])
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

    def get_assembly_code_from_evapro(self, assembly):
        query = select(AssemblySet.assembly_code)\
            .join(AccessionedAssembly, AssemblySet.assembly_set_id==AccessionedAssembly.assembly_set_id)\
            .where(AccessionedAssembly.assembly_accession==assembly)
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

    def eva_study_accession_from_project_accession_code(self):
        # Is it to really necessary
        '''$sth_eva = $dbh_eva->prepare("select project_accession_code from project");
                $sth_eva->execute();
                while(my @row1 = $sth_eva->fetchrow_array()){
                    $accession_to_id->{'PROJECT_ID'} = $row1[0];
                    my $e = $row1[0] + 1;
                    my $sth2_eva = $dbh_eva->prepare("update project set eva_study_accession=".$e." where project_accession = ?");
                    $sth2_eva->execute($row->{'PROJECT_ID'});
                }'''
        pass

    def insert_project_in_evapro(self, project_accession, center_name, project_alias, title, description,
                                 ena_study_type, ena_secondary_study_id, scope='multi-isolate', material='DNA',
                                 type='other'):
        query = select(Project).where(Project.project_accession == project_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            project_obj = result.Project
        else:
            project_obj = Project(
                project_accession=project_accession, center_name=center_name, alias=project_alias, title=title,
                description=description, scope=scope, material=material, type=type, study_type=ena_study_type,
                secondary_study_id=ena_secondary_study_id
            )
            self.eva_session.add(project_obj)
            self.info(f'Add Project {project_accession} to EVAPRO')
        return project_obj

    def insert_publications_in_evapro(self):
        '''foreach my $pp(@publicat){
            $sth_eva = $dbh_eva->prepare("select dbxref_id from dbxref where db=? and id=? and source_object=?");
            $sth_eva->execute($pp->{'db'},$pp->{'id'},'project');
            my $dbxref = {};
            while(my @row1 = $sth_eva->fetchrow_array()){
                $dbxref->{'dbxref_id'} = $row1[0];
            }
            if (!defined $dbxref->{'dbxref_id'}){
                $sth_eva = $dbh_eva->prepare("insert into dbxref (db,id,link_type,source_object) values (?,?,?,?) returning dbxref_id");
                $sth_eva->execute($pp->{'db'},$pp->{'id'},'publication','project');
                $dbxref = $sth_eva->fetchrow_hashref();
            }
            my $sth2_eva = $dbh_eva->prepare("insert into project_dbxref(project_accession,dbxref_id) values (?,?)");
            $sth2_eva->execute($project_accession,$dbxref->{'dbxref_id'});
        }'''

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
        return linked_project_obj

    def insert_ena_submission(self, ena_submission_accession, action, submission_alias, submission_date, brokered=True, submission_type='PROJECT'):
        query = select(Submission).where(Submission.submission_accession == ena_submission_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            submission_obj = result.Submission
        else:
            submission_obj = Submission(
                submission_accession=ena_submission_accession, action=action, title=submission_alias, date=submission_date, brokered=brokered, type=submission_type
            )
            self.eva_session.add(submission_obj)
            self.info(f'Add Submission {ena_submission_accession} {action} to EVAPRO')
        return submission_obj

    def insert_project_ena_submission(self, project_obj, submission_obj, eload):
        '''
        $sth_eva = $dbh_eva->prepare("insert into PROJECT_ENA_SUBMISSION (PROJECT_ACCESSION,SUBMISSION_ID) values (?,?)");
        foreach my $sub_id(@{$accession_to_id->{'SUBMISSION_ID'}}){
            $sth_eva->execute($project_accession,$sub_id);
        }
        $sth_eva = $dbh_eva->prepare("insert into EVA_SUBMISSION (EVA_SUBMISSION_ID,EVA_SUBMISSION_STATUS_ID) values (?,?)");
        $sth_eva->execute($eload,6);
        $sth_eva = $dbh_eva->prepare("insert into PROJECT_EVA_SUBMISSION (PROJECT_ACCESSION,old_ticket_id,ELOAD_ID) values (?,?,?)");
        $sth_eva->execute($project_accession,$eload,$eload);
        '''
        ProjectEnaSubmission(project_accession=project_obj.project_accession, submission_id=submission_obj.submission_id)
        EvaSubmission(eva_submission_id=eload, eva_submission_status_id=6)
        ProjectEvaSubmission(project_accession=project_obj.project_accession, old_ticket_id=eload, eload_id=eload)


    def insert_analysis(self, analysis_accession, title, alias, description, center_name, date, assembly_set_id):
        query = select(Analysis).where(Analysis.analysis_accession == analysis_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            analysis_obj = result.Analysis
        else:
            analysis_obj = Analysis(analysis_accession=analysis_accession, title=title, alias=alias, description=description,
                     center_name=center_name, date=date, assembly_set_id=assembly_set_id)
            self.eva_session.add(analysis_obj)
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
            assembly_set_obj = AssemblySet(taxonomy_id=taxonomy_obj.taxonomy_id, assembly_name=assembly_name, assembly_code=assembly_code)
            self.eva_session.add(assembly_set_obj)
            self.eva_session.flush()

            accessioned_assembly_obj = AccessionedAssembly(
                assembly_set_id=assembly_set_obj.assembly_set_id, assembly_accession=assembly_accession,
                assembly_chain=assembly_accession.split('.')[0], assembly_version=assembly_accession.split('.')[1]
            )
            self.eva_session.add(accessioned_assembly_obj)

        return assembly_set_obj

    def insert_custom_assembly_set(self, taxonomy_obj, custom_assembly):
        '''
        $sth_eva = $dbh_eva->prepare("select assembly_set_id from custom_assembly where assembly_location=? and assembly_file_name=?");
                        my @tmp_asemb = split(/\//,$row->{'CUSTOM'});
                        my $fi = pop(@tmp_asemb);
                        $sth_eva->execute(join("/",@tmp_asemb),$fi);
                        my $assset_id = -1;
                        while (my @rowz = $sth_eva->fetchrow_array()){
                            $assset_id = $rowz[0];
                        }
                        if ($assset_id == -1){
                            $sth_eva = $dbh_eva->prepare("select taxonomy_id from project_taxonomy where project_accession=?");
                            $sth_eva->execute($project_accession);
                            my $tmp_tax = -1;
                            while (my @rowz = $sth_eva->fetchrow_array()){
                                $tmp_tax = $rowz[0];
                            }
                            if ($tmp_tax == -1){
                                warn "Unable to determine taxonomy for project!";
                                exit(1);
                            }
                            $sth_eva = $dbh_eva->prepare("select assembly_set_id from assembly_set where taxonomy_id=? and assembly_name=?");
                            $sth_eva->execute($tmp_tax,$fi);
                            while (my @rowz = $sth_eva->fetchrow_array()){
                                $assset_id = $rowz[0];
                            }
                            if ($assset_id == -1){
                                $sth_eva = $dbh_eva->prepare("insert into assembly_set (taxonomy_id,assembly_name) values (?,?)");
                                $sth_eva->execute($tmp_tax,$fi);
                                $sth_eva = $dbh_eva->prepare("select assembly_set_id from assembly_set where taxonomy_id=? and assembly_name=?");
                                $sth_eva->execute($tmp_tax,$fi);
                                while(my @rowz = $sth_eva->fetchrow_array()){
                                    $assset_id = $rowz[0];
                                }
                                $sth_eva = $dbh_eva->prepare("insert into custom_assembly (assembly_set_id,assembly_location,assembly_file_name) values (?,?,?)");
                                $sth_eva->execute($assset_id,join("/",@tmp_asemb),$fi);
                            } else {
                                warn "custom assembly appears to be used already, but location different";
                                warn $fi;
                                exit(1);
                            }

                        }
                        $sth_eva = $dbh_eva->prepare("insert into ANALYSIS (ANALYSIS_ACCESSION,CENTER_NAME,ALIAS,TITLE,DESCRIPTION,DATE,assembly_set_id) values (?,?,?,?,?,?,?)");
                        $sth_eva->execute($row->{'ANALYSIS_ID'},$row->{'CENTER_NAME'},$row->{'ANALYSIS_ALIAS'},$row->{'ANALYSIS_TITLE'},$row->{'DESCRIPTION'},$row->{'FIRST_CREATED'},$assset_id);
                        $browsable_assembly_set_id = $assset_id;
        '''
        pass

    def get_file(self, file_md5):
        query = select(File).where(File.file_md5 == file_md5)
        result = self.eva_session.execute(query).fetchone()
        if result:
            return result.File
        return None

    def insert_file(self, project_accession, assembly_set_id, ena_submission_file_id, filename, file_md5, file_type,
                    ftp_file, file_location=None, file_class='submitted', file_version=1, is_current=True):
        file_obj = self.get_file(file_md5)
        if not file_obj:
            file_obj = File(
                ena_submission_file_id=ena_submission_file_id, filename=filename, file_md5=file_md5, file_type=file_type,
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
            self.info(f'Add Sample type {biosample_accession} to EVAPRO')

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

    def load_project_from_ena(self, project_accession):
        self.eva_session.begin()

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
        parent_project = self.ena_project_finder.find_parent_project(project_accession=project_accession)
        if parent_project:
            self.insert_linked_projects(project_obj, parent_project)

        ###
        # LOAD TAXONOMY
        ###
        taxonomy_obj = self.insert_taxonomy(taxonomy_id)
        project_obj.taxonomies.append(taxonomy_obj)

        ###
        # LOAD SUBMISSIONS
        ###
        for submission_info in self.ena_project_finder.find_ena_submission(project_accession=project_accession):
            submission_id, alias, last_updated, hold_date, action = submission_info
            # action {"type": ADD, "schema": project, "source": ELOAD.Project.xml}
            submission_obj = self.insert_ena_submission(ena_submission_accession=submission_id, action=action.get('type'),
                                       submission_alias=alias, submission_date=last_updated, brokered=True,
                                       submission_type=action.get('schema').upper())
            # TODO: Link analysis with submission
        ###
        # LOAD ANALYSIS
        ###
        for analysis_info in self.ena_project_finder.find_analysis_in_ena(project_accession=project_accession):
            (
                analysis_accession, analysis_title, analysis_alias, analysis_description, analysis_type, center_name,
                first_created, assembly, refname, custom, experiment_types, platforms
            ) = analysis_info
            assembly_set_obj = self.insert_assembly_set(taxonomy_obj=taxonomy_obj, assembly_accession=assembly)
            analysis_obj = self.insert_analysis(
                analysis_accession=analysis_accession, title=analysis_title,alias=analysis_alias,
                description=analysis_description, center_name=center_name, date=first_created,
                assembly_set_id=assembly_set_obj.assembly_set_id)

            ###
            # LINK PLATFORMS (ASSUME NO NEW PLATFORM)
            ###
            platform_objs = [self.get_platform_obj_from_evapro(platform) for platform in platforms]
            analysis_obj.platforms = platform_objs

            ###
            # LOAD EXPERIMENT TYPE
            ###
            experiment_type_objs = [self.insert_experiment_type(experiment_type) for experiment_type in experiment_types]
            analysis_obj.experiment_types = experiment_type_objs

            ###
            # LOAD FILE
            ###
            for file_info in self.ena_project_finder.find_files_in_ena(analysis_accession=analysis_accession):
                analysis_accession, submission_file_id, filename, file_md5, file_type, status_id = file_info
                ftp_file = get_ftp_path(filename=filename, accession_id=submission_file_id)
                file_obj = self.insert_file(
                    project_accession=project_accession,
                    assembly_set_id=assembly_set_obj.assembly_set_id,
                    ena_submission_file_id=submission_file_id,
                    filename=filename,
                    file_md5=file_md5,
                    file_type=file_type,
                    ftp_file=ftp_file
                )

            ###
            # LOAD SAMPLE
            ###
            for sample_info in self.ena_project_finder.find_samples_in_ena(analysis_accession=analysis_accession):
                sample_id, sample_accession = sample_info
                sample_obj = self.insert_sample(biosample_accession=sample_accession, ena_accession=sample_id)
        self.eva_session.commit()
        self.eva_session.close()

    def load_samples_from_vcf_file(self, sample_name_2_sample_accession, vcf_file, vcf_file_md5):
        sample_names = get_samples_from_vcf(vcf_file)
        self.eva_session.begin()
        file_obj = self.get_file(vcf_file_md5)
        if not file_obj:
            self.error(f'Cannot find file {vcf_file} in EVAPRO for md5 {vcf_file_md5}')
            return
        for sample_name in sample_names:
            sample_accession = sample_name_2_sample_accession.get(sample_name)
            sample_obj = self.get_sample(sample_accession)
            if not sample_obj:
                self.error(f'Cannot find sample {sample_accession} in EVAPRO')
                return
            self.insert_sample_in_file(file_id=file_obj.file_id, sample_id=sample_obj.sample_id,
                                       name_in_file=sample_name)
        self.eva_session.commit()



