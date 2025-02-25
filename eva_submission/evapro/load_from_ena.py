import re
from functools import cached_property
import xml.etree.ElementTree as ET
from urllib.parse import urlsplit

import oracledb
from ebi_eva_common_pyutils.assembly_utils import is_patch_assembly
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.ena_utils import get_scientific_name_and_common_name
from ebi_eva_common_pyutils.ncbi_utils import get_ncbi_assembly_name_from_term
from ebi_eva_internal_pyutils.config_utils import get_metadata_creds_for_profile
from ebi_eva_internal_pyutils.metadata_utils import build_taxonomy_code
from sqlalchemy import select, create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session

from eva_submission.evapro.table import Project, Taxonomy, LinkedProject, Submission, ProjectEnaSubmission, \
    EvaSubmission, ProjectEvaSubmission, Analysis, AssemblySet, AccessionedAssembly, File, BrowsableFile, metadata


def get_ftp_path(filename, prefix_path, accession_id):
    return f"{prefix_path}/{accession_id[0:6]}/{accession_id}/{filename}"


class EnaProjectLoader():

    @cached_property
    def era_connection(self):
        pass

    def era_cursor(self):
        return self.era_connection.cursor()

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

    @staticmethod
    def _parse_study_description_from_xml(study_xml):
        root = ET.fromstring(study_xml)

        # Extract Study Description
        doc = root.find(".//STUDY_DESCRIPTION")
        if doc:
            return doc.text
        else:
            return ''

    @staticmethod
    def _parse_actions_and_alias_from_submission_xml(submission_xml):
        root = ET.fromstring(submission_xml)
        submission = root.find(".//SUBMISSION")
        submission_alias = submission.attrib.get("alias") if submission is not None else None

        actions = []
        hold_date = None
        for action in root.findall(".//ACTION"):
            for child in action:
                action_type = child.tag
                if action_type == "HOLD":
                    hold_date = child.attrib.get("HoldUntilDate")
                    continue
                schema = child.attrib.get("schema")
                source = child.attrib.get("source")
                actions.append({"type": action_type, "schema": schema, "source": source})
        if hold_date:
            for action in actions:
                action["hold_date"] = hold_date
        return submission_alias, actions

    @staticmethod
    def _parse_submission_from_xml(submission_xml):
        root = ET.fromstring(submission_xml)

        # Extract Study Description
        doc = root.find(".//STUDY_DESCRIPTION")
        if doc:
            return doc.text
        else:
            return ''

    def _parse_analysis_description_and_type_from_xml(self, analysis_xml):
        root = ET.fromstring(analysis_xml)
        description = root.find(".//DESCRIPTION").text if root.find(".//DESCRIPTION") is not None else None

        # Extract Analysis Type and associated elements
        analysis_type_element = root.find(".//ANALYSIS_TYPE")
        platform = None
        experiment_type = None

        if analysis_type_element is not None:
            # Get the first child of ANALYSIS_TYPE (e.g., SEQUENCE_VARIATION)
            for child in analysis_type_element:
                # Extract associated elements
                for sub_element in child:
                    if sub_element.tag == 'EXPERIMENT_TYPE':
                        experiment_type = sub_element.text
                    if sub_element.tag == 'PLATFORM':
                        platform = sub_element.text
        return description, experiment_type, platform

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

    def find_project_from_ena_database(self, project_accession):
        era_project_query = (
            'select s.study_id, project_id, s.submission_id, p.center_name, p.project_alias, s.study_type, '
            'p.first_created, p.project_title, p.tax_id, p.scientific_name, p.common_name, '
            'xmltype.getclobval(STUDY_XML) study_xml, xmltype.getclobval(PROJECT_XML) project_xml '
            'from era.PROJECT p '
            'left outer join era.STUDY s using(project_id) '
            f"where project_id='{project_accession}'"
        )
        with self.era_cursor() as cursor:
            results = list(cursor.execute(era_project_query))
            assert len(results) == 1, f'{len(results)} project accession found in ERA for {project_accession}'
        (
            study_id, project_accession, submission_id, center_name, project_alias, study_type,
            first_created, project_title, taxonomy_id, scientific_name, common_name,  study_xml, project_xml
        ) = results[0]
        study_description = self._parse_study_description_from_xml(str(study_xml))
        # Project publication used to be parsed from the project XML. but there were none in the EVA Projects
        # project_publications ['PROJECT_XML/PUBLICATIONS/PUBLICATION/PUBLICATION_LINKS/PUBLICATION_LINK/[DB:ID]']
        return (
            study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
            str(project_title), taxonomy_id, scientific_name, common_name, study_description
        )


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

    def find_parent_project(self, project_accession):
        # link_type=2 == project
        # link_role=1 == hierarchical
        era_linked_project_query = (f"select to_id from era.ena_link "
                                    f"where TO_LINK_TYPE_ID=2 AND LINK_ROLE_ID=1 AND from_id='{project_accession}'")
        with self.era_cursor() as cursor:
            parent_project = [to_id for to_id, in cursor.execute(era_linked_project_query)]
            if not parent_project:
                return None
            elif len(parent_project) > 1:
                raise RuntimeError(f"Multiple parent projects found for project accession {project_accession}")
            else:
                return parent_project[0]

    def find_ena_submission(self, project_accession):
        era_submission_query = (
            "select submission.submission_id, xmltype.getclobval(SUBMISSION_XML) submission_xml, "
            "submission.last_updated UPDATED "
            "from era.submission "
            "left outer join era.study on submission.submission_id=study.submission_id "
            f"where study.project_id='{project_accession}' and study.submission_id like 'ERA%'"
        )
        with self.era_cursor() as cursor:
            for results in cursor.execute(era_submission_query):
                (submission_id, submission_xml, last_updated) = results
                alias, actions = self._parse_actions_and_alias_from_submission_xml(str(submission_xml))
                for action in actions:
                    yield submission_id, alias, last_updated, action

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
        query = select(Project).where(Taxonomy.taxonomy_id == taxonomy_id)
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
        return taxonomy_obj

    def insert_project_taxonomy(self, project_obj, taxonomy_obj):
        project_obj.taxonomy = taxonomy_obj

    def insert_linked_projects(self, project_obj, linked_project_accession, project_relation='PARENT'):
        '''
        $sth_ena = $dbh_ena->prepare("select to_id from era.ena_link where from_id=?");
            $sth_ena->execute($project_accession);
            while(my $row = $sth_ena->fetchrow_hashref()){
                $sth_eva = $dbh_eva->prepare("insert into LINKED_PROJECT (PROJECT_ACCESSION,LINKED_PROJECT_ACCESSION,LINKED_PROJECT_RELATION) values (?,?,?)");
                $sth_eva->execute($project_accession,$row->{'TO_ID'},'PARENT');
            }
        '''
        linked_project_obj = LinkedProject(project_accession=project_obj.project_accession,
                                           linked_project_accession=linked_project_accession,
                                           linked_project_relation=project_relation)
        return linked_project_obj

    def insert_ena_submission(self, ena_submission_accession, action, submission_alias, submission_date, brokered=True, submission_type='PROJECT'):
        query = select(Submission).where(Submission.submission_accession == ena_submission_accession,
                                         Submission.action==action)
        result = self.eva_session.execute(query).fetchone()
        if result:
            submission_obj = result.Submission
        else:
            submission_obj = Submission(
                submission_accession=ena_submission_accession, action=action, title=submission_alias, date=submission_date, brokered=brokered, type=submission_type
            )
            self.eva_session.add(submission_obj)
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


    def find_analysis_in_ena(self, project_accession):
        era_analysis_query = (
            'select t.analysis_id, t.analysis_title, t.analysis_alias, t.analysis_type, t.center_name, t.first_created, '
            ' xmltype.getclobval(t.ANALYSIS_XML) analysis_xml, x.assembly assembly, x.refname refname, y.custom '
            'from era.analysis t '
            "left outer join XMLTABLE('/ANALYSIS_SET//ANALYSIS_TYPE//SEQUENCE_VARIATION//ASSEMBLY//STANDARD' passing t.analysis_xml columns assembly varchar2(2000) path \'@accession\', refname varchar2(2000) path \'@refname\') x on (1=1) "
            "left outer join XMLTABLE('/ANALYSIS_SET//ANALYSIS_TYPE//SEQUENCE_VARIATION//ASSEMBLY//CUSTOM//URL_LINK' passing t.analysis_xml columns custom varchar2(2000) path \'URL\') y on (1=1) "
            "where t.status_id <> 5 and lower(SUBMISSION_ACCOUNT_ID) in ('webin-1008') "
            f"and (study_id in (select study_id from era.study where project_id='{project_accession}') "
            f"or study_id='{project_accession}' or bioproject_id='{project_accession}')"
        )
        with (self.era_cursor() as cursor):
            for results in cursor.execute(era_analysis_query):
                (
                    analysis_id, analysis_title, analysis_alias, analysis_type, center_name, first_created,
                    analysis_xml,  assembly, refname, custom
                ) = results
                analysis_description, experiment_type, platform = self._parse_analysis_description_and_type_from_xml(str(analysis_xml))
                if analysis_type != 'SEQUENCE_VARIATION':
                    continue
                yield (
                    analysis_id, analysis_title, analysis_alias, analysis_description, analysis_type, center_name,
                    first_created, assembly, refname, custom, experiment_type, platform
                )

    def insert_analysis(self, analysis_accession, title, alias, description, center_name, date, assembly_set_id):
        query = select(Analysis).where(Analysis.analysis_accession == analysis_accession)
        result = self.eva_session.execute(query).fetchone()
        if result:
            analysis_obj = result.Submission
        else:

            analysis_obj = Analysis(analysis_accession=analysis_accession, title=title, alias=alias, description=description,
                     center_name=center_name, date=date, assembly_set_id=assembly_set_id)
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

            accessioned_assembly_obj = AccessionedAssembly(
                assembly_set_id=assembly_set_obj.assembly_set_id, assembly_accession=assembly_accession,
                assembly_chain=assembly_accession.split('.')[0], assembly_version=assembly_accession.split('.')[1]
            )
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

    def find_samples_in_ena(self, analysis_accession):
        query = ("select s.sample_id, s.biosample_id from era.analysis_sample asa "
                 f"join era.sample s on asa.sample_id = s.sample_id where analysis_id='{analysis_accession}'")
        with self.era_cursor() as cursor:
            for sample_id, sample_accession in cursor.execute(query):
                yield sample_id, sample_accession

    def find_files_in_ena(self, analysis_accession):
        query = (
            "select distinct asf.analysis_id as analysis_accession, wf.submission_file_id as submission_file_id, "
            "regexp_substr(wf.data_file_path, \'[^/]*$\') as filename,"
            "wf.checksum as file_md5, wf.data_file_format as file_type, ana.status_id "
            "from era.analysis_submission_file asf "
            "join era.webin_file wf on asf.analysis_id=wf.data_file_owner_id "
            f"join era.analysis ana on asf.analysis_id=ana.analysis_id where asf.analysis_id='{analysis_accession}'"
        )

        with self.era_cursor() as cursor:
            for analysis_accession, submission_file_id, filename, file_md5, file_type, status_id in cursor.execute(query):
                yield analysis_accession, submission_file_id, filename, file_md5, file_type, status_id

    def insert_file(self, project_accession, assembly_set_id, ena_submission_file_id, filename, file_md5, file_type,
                    file_location, file_class, file_version,
                    is_current, ftp_file):
        query = select(File).where(File.file_md5 == file_md5)
        result = self.eva_session.execute(query).fetchone()
        if result:
            file_obj = result.File
        else:
            file_obj = File(
                ena_submission_file_id=ena_submission_file_id, filename=filename, file_md5=file_md5, file_type=file_type,
                file_location=file_location, file_class=file_class, file_version=file_version, is_current=is_current,
                ftp_file=ftp_file
            )
            self.eva_session.session.add(file_obj)
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
                self.eva_session.session.add(browsable_file_obj)
        return file_obj

    def load_project_from_ena(self, project_accession):

        (
            study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
            project_title, taxonomy_id, scientific_name, common_name, study_description
        ) = self.find_project_from_ena_database(project_accession)
        project_obj = self.insert_project_in_evapro(
            project_accession=project_accession, center_name=center_name, project_alias=project_alias,
            title=project_title, description=study_description, ena_study_type=study_type,
            ena_secondary_study_id=study_id
        )

        taxonomy_obj = self.insert_taxonomy(taxonomy_id)
        project_obj.taxonomy = taxonomy_obj
        for submission_info in self.find_ena_submission(project_accession=project_accession):
            submission_id, alias, last_updated, action = submission_info
            # action {"type": action_type, "schema": schema, "source": source}
            submission_obj = self.insert_ena_submission(ena_submission_accession=submission_id, action=action.get('type'),
                                       submission_alias=alias, submission_date=last_updated, brokered=True,
                                       submission_type=action.get('schema').upper())


            # TODO: Link analysis with submission
        for analysis_info in self.find_analysis_in_ena(project_accession=project_accession):
            (
                analysis_id, analysis_title, analysis_alias, analysis_description, analysis_type, center_name,
                first_created, assembly, refname, custom, experiment_type, platform
            ) = analysis_info
            assembly_set_obj = self.insert_assembly_set(taxonomy_obj=taxonomy_obj, assembly_accession=assembly)
            analysis_obj = self.insert_analysis(
                analysis_accession=analysis_id, title=analysis_title,alias=analysis_alias,
                description=analysis_description, center_name=center_name, date=first_created,
                assembly_set_id=assembly_set_obj.assembly_set_id)
            # self.get_platform(platform)
            # analysis_obj.platforms = platform_list


