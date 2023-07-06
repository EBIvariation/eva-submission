import os
import re
from datetime import datetime, timedelta
from functools import cached_property
from io import BytesIO
from xml.dom import minidom
from xml.etree.ElementTree import Element, ElementTree

from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.eload_utils import check_existing_project_in_ena
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


def today():
    return datetime.today()


def prettify(etree):
    """Return a pretty-printed XML string for the ElementTree. """
    outfile = BytesIO()
    etree.write(outfile, encoding='utf-8', xml_declaration=True)
    rough_string = outfile.getvalue()
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent='    ', encoding="utf-8")


def add_attributes(element, **kwargs):
    for key in kwargs:
        if kwargs[key] and kwargs[key].strip():
            element.attrib[key] = kwargs[key].strip()


def add_element(parent_element, element_name, element_text=None, content_required=False, **kwargs):
    elemt = None
    if parent_element is not None:
        if element_text or kwargs or not content_required:
            elemt = Element(element_name)
            if element_text:
                elemt.text = element_text.strip()
            add_attributes(elemt, **kwargs)
            parent_element.append(elemt)

    return elemt


def add_links(links_elemt, links, link_type=None):
    for link in links:
        link_type_elemt = add_element(links_elemt, link_type)
        if re.match('^(ftp:|http:|file:|https:)', link):
            # TODO: requirement in the format
            url, label = link.split('|')
            url_link_elemt = add_element(link_type_elemt, 'URL_LINK')
            add_element(url_link_elemt, 'LABEL', element_text=label)
            add_element(url_link_elemt, 'URL', element_text=url)
        else:
            xlink_elemt = add_element(link_type_elemt, 'XREF_LINK')
            # TODO: requirement in the format
            # TODO: Verify the format of the LINK because there is a mismatch between the perl code
            #  and the help in the metadata spreadhseet
            link_parts = link.split(':')
            add_element(xlink_elemt, 'DB', element_text=link_parts[0])
            if len(link_parts) > 1:
                add_element(xlink_elemt, 'ID', element_text=link_parts[1])
            if len(link_parts) > 2:
                add_element(xlink_elemt, 'LABEL', element_text=link_parts[2])


def add_attribute_elements(analysis_elemt, data_row, object_type):
    attributes_elemt = add_element(analysis_elemt, object_type + '_ATTRIBUTES')

    if 'Attribute(s)' in data_row:
        attributes = data_row.get('Attribute(s)').split(',')
        for attribute in attributes:
            attribute_parts = attribute.split(':')
            attribute_elemt = add_element(attributes_elemt, object_type + '_ATTRIBUTE')
            add_element(attribute_elemt, 'TAG', element_text=attribute_parts[0])
            add_element(attribute_elemt, 'VALUE', element_text=attribute_parts[1])
            if len(attribute_parts) > 2:
                add_element(attribute_elemt, 'UNITS', element_text=attribute_parts[2])
    return attributes_elemt


class EnaXlsxConverter(AppLogger):

    def __init__(self, metadata_file, output_folder, name):
        self.metadata_file = metadata_file
        self.output_folder = output_folder
        self.name = name
        self.reader = EvaXlsxReader(self.metadata_file)

        self.project_file = os.path.join(self.output_folder, self.name + '.Project.xml')
        self.analysis_file = os.path.join(self.output_folder, self.name + '.Analysis.xml')
        self.submission_file = os.path.join(self.output_folder, self.name + '.Submission.xml')
        self.single_submission_file = os.path.join(self.output_folder, self.name + '.SingleSubmission.xml')

    @cached_property
    def is_existing_project(self):
        return self.existing_project is not None

    @cached_property
    def existing_project(self):
        prj_alias = self.reader.project.get('Project Alias', '')
        prj_title = self.reader.project.get('Project Title', '')
        if re.match(r'^PRJ(EB|NA)', prj_alias) and check_existing_project_in_ena(prj_alias):
            return prj_alias
        elif re.match(r'^PRJ(EB|NA)', prj_title) and check_existing_project_in_ena(prj_title):
            return prj_title
        return None

    def _create_project_xml(self):
        """
        This function read the project row from the XLS parser then create and populate an XML element following ENA
        data model.
        :return: The top XML element
        """
        project_row = self.reader.project
        root = Element('PROJECT_SET')

        project_elemt = add_element(root, 'PROJECT',
                                    alias=project_row.get('Project Alias'),
                                    accession=project_row.get('Project Accession'),
                                    center_name=project_row.get('Center'))

        add_element(project_elemt, 'TITLE', project_row.get('Project Title'), content_required=True)
        add_element(project_elemt, 'DESCRIPTION', project_row.get('Description'), content_required=True)
        if 'Publication(s)' in project_row and project_row.get('Publication(s)'):
            publications_elemt = add_element(project_elemt, 'PUBLICATIONS')
            publications = project_row.get('Publication(s)').strip().split(',')
            for publication in publications:
                pub_elemt = add_element(publications_elemt, 'PUBLICATION')
                pub_links_elemt = add_element(pub_elemt, 'PUBLICATION_LINKS')
                pub_link_elemt = add_element(pub_links_elemt, 'PUBLICATION_LINK')
                xref_link_elemt = add_element(pub_link_elemt, 'XREF_LINK')
                # Assuming format like PubMed:123456
                # TODO: requirement in the format
                pub_db, pub_id = publication.split(':')
                add_element(xref_link_elemt, 'DB', element_text=pub_db)
                add_element(xref_link_elemt, 'ID', element_text=pub_id)

        if 'Collaborator(s)' in project_row and project_row.get('Collaborator(s)'):
            collaborators = project_row.get('Collaborator(s)').strip().split(',')
            collaborators_elemt = add_element(project_elemt, 'COLLABORATORS')
            for collaborator in collaborators:
                add_element(collaborators_elemt, 'COLLABORATOR', element_text=collaborator)

        sub_project_elemt = add_element(project_elemt, 'SUBMISSION_PROJECT')
        add_element(sub_project_elemt, 'SEQUENCING_PROJECT')

        if 'Tax ID' in project_row:
            org_elemt = add_element(sub_project_elemt, 'ORGANISM')
            add_element(org_elemt, 'TAXON_ID', element_text=str(project_row.get('Tax ID')).strip())
            scientific_name = get_scientific_name_from_ensembl(str(project_row.get('Tax ID')).strip())
            add_element(org_elemt, 'SCIENTIFIC_NAME', element_text=scientific_name)

            add_element(org_elemt, 'STRAIN', element_text=project_row.get('Strain', ''), content_required=True)
            add_element(org_elemt, 'BREED', element_text=project_row.get('Breed', ''), content_required=True)

        if project_row.get('Parent Project(s)') or \
                project_row.get('Child Project(s)') or \
                project_row.get('Peer Project(s)'):
            related_prjs_elemt = add_element(project_elemt, 'RELATED_PROJECTS')

            if 'Parent Project(s)' in project_row and project_row.get('Parent Project(s)'):
                parent_prjs = project_row.get('Parent Project(s)').split(',')
                for parent_prj in parent_prjs:
                    related_prj_elemt = add_element(related_prjs_elemt, 'RELATED_PROJECT')
                    add_element(related_prj_elemt, 'PARENT_PROJECT', accession=parent_prj)

            if 'Child Project(s)' in project_row and project_row.get('Child Project(s)'):
                children_prjs = project_row.get('Child Project(s)').split(',')
                for child_prj in children_prjs:
                    related_prj_elemt = add_element(related_prjs_elemt, 'RELATED_PROJECT')
                    add_element(related_prj_elemt, 'CHILD_PROJECT', accession=child_prj)

            if 'Peer Project(s)' in project_row and project_row.get('Peer Project(s)'):
                peer_prjs = project_row.get('Peer Project(s)').split(',')
                for peer_prj in peer_prjs:
                    related_prj_elemt = add_element(related_prjs_elemt, 'RELATED_PROJECT')
                    add_element(related_prj_elemt, 'PEER_PROJECT', accession=peer_prj)

        if 'Link(s)' in project_row and project_row.get('Link(s)'):
            links_elemt = add_element(project_elemt, 'PROJECT_LINKS')
            project_links = project_row.get('Link(s)').split(',')
            add_links(links_elemt, project_links, link_type='PROJECT_LINK')

        # TODO: Is this still relevant because it is not documented in the metadata template
        add_attribute_elements(project_elemt, project_row, object_type='PROJECT')
        return root

    def _create_analysis_xml(self):
        """
        This function reads the analysis rows from the XLS parser then create and populate an XML element following ENA data
        model.
        :return: The top XML element
        """
        root = Element('ANALYSIS_SET')
        for analysis_row in self.reader.analysis:
            sample_rows = self.reader.samples_per_analysis[analysis_row.get('Analysis Alias')]
            file_rows = self.reader.files_per_analysis[analysis_row.get('Analysis Alias')]
            self._add_analysis(root, analysis_row, self.reader.project, sample_rows, file_rows)
        return root

    def _add_analysis(self, root, analysis_row, project_row, sample_rows, file_rows):
        """
        Add an analysis element using information from the analysis row provided.
        The element is appended to the root element provided
        :param root: Top level element
        :param analysis_row: Dictionary representing one row of the analysis tab in the XLS file
        :param project_row: Dictionary representing one row of the project tab in the XLS file
        :param sample_rows: Dictionary representing one row of the sample tab in the XLS file
        :param file_rows: Dictionary representing one row of the file tab in the XLS file
        :return: None
        """
        analysis_elemt = add_element(root, 'ANALYSIS',
                                     alias=analysis_row.get('Analysis Alias'),
                                     accession=analysis_row.get('Analysis Accession'),
                                     broker_name=analysis_row.get('Broker'),
                                     center_name=analysis_row.get('Centre') or project_row.get('Center'))

        add_element(analysis_elemt, 'TITLE', element_text=analysis_row.get('Analysis Title'))
        add_element(analysis_elemt, 'DESCRIPTION', element_text=analysis_row.get('Description'))

        # Add Project link
        study_attr = {}
        if re.match(r'^PRJ(EB|NA)', project_row.get('Project Alias', '')):
            study_attr = {'accession': project_row.get('Project Alias')}
        elif re.match(r'^PRJ(EB|NA)', project_row.get('Project Title', '')):
            study_attr = {'accession': project_row.get('Project Title')}
        elif project_row.get('Project Alias'):
            study_attr = {'refname': project_row.get('Project Alias')}
        elif project_row.get('Project Title'):
            study_attr = {'refname': project_row.get('Project Title')}
        add_element(analysis_elemt, 'STUDY_REF', **study_attr)

        # Add sample information for this analysis
        for sample_row in sample_rows:
            add_element(analysis_elemt, 'SAMPLE_REF',
                        accession=sample_row.get('Sample Accession'),
                        label=sample_row.get('Sample ID'))

        # Add run accessions
        if 'Run Accession(s)' in analysis_row and analysis_row.get('Run Accession(s)'):
            for run in analysis_row.get('Run Accession(s)').split(','):
                add_element(analysis_elemt, 'RUN_REF', accession=run)

        # Add analysis type
        anal_type_elemt = add_element(analysis_elemt, 'ANALYSIS_TYPE')
        seq_var_elemt = add_element(anal_type_elemt, 'SEQUENCE_VARIATION')
        assembly_elemt = add_element(seq_var_elemt, 'ASSEMBLY')
        if analysis_row.get('Reference').split(':')[0] in ['file', 'http', 'ftp']:
            custom_elemt = add_element(assembly_elemt, 'CUSTOM')
            url_link_elemt = add_element(custom_elemt, 'URL_LINK')
            add_element(url_link_elemt, 'URL', element_text=analysis_row.get('Reference').strip())
        else:
            add_element(assembly_elemt, 'STANDARD', accession=analysis_row.get('Reference').strip())
        # TODO: Check if the Sequence section needs to be supported.
        #  There was a section in the perl code that added SEQUENCE elements for each contig

        experiments = analysis_row.get('Experiment Type').strip().split(':')
        for experiment in experiments:
            add_element(seq_var_elemt, 'EXPERIMENT_TYPE', element_text=experiment.lower().capitalize())
        add_element(seq_var_elemt, 'PROGRAM', element_text=analysis_row.get('Software'), content_required=True)
        if 'Platform' in analysis_row and analysis_row.get('Platform'):
            platforms = analysis_row.get('Platform').strip().split(',')
            for platform in platforms:
                add_element(seq_var_elemt, 'PLATFORM', element_text=platform)
        if 'Imputation' in analysis_row and analysis_row.get('imputation') \
                and str(analysis_row.get('imputation').strip()) == '1':
            add_element(seq_var_elemt, 'IMPUTATION', element_text='1')

        files_elemt = add_element(analysis_elemt, 'FILES')
        for file_row in file_rows:
            add_element(files_elemt, 'FILE',
                        filename=file_row['File Name'],
                        filetype=file_row['File Type'],
                        checksum_method='MD5',
                        checksum=file_row['MD5'])

        if 'Link(s)' in analysis_row and analysis_row.get('Link(s)'):
            analysis_links_elemt = add_element(analysis_elemt, 'ANALYSIS_LINKS')
            analysis_links = analysis_row.get('Link(s)').strip().split(',')
            add_links(analysis_links_elemt, analysis_links, link_type='ANALYSIS_LINK')

        # TODO: Is this still relevant because it is not documented in the metadata template
        analysis_attributes_elemt = add_attribute_elements(analysis_elemt, analysis_row, object_type='ANALYSIS')
        if 'Pipeline Description' in analysis_row and analysis_row.get('Pipeline Description'):
            analysis_attrib_elemt = add_element(analysis_attributes_elemt, 'ANALYSIS_ATTRIBUTE')
            add_element(analysis_attrib_elemt, 'TAG', element_text='Pipeline_Description')
            add_element(analysis_attrib_elemt, 'VALUE', element_text=analysis_row.get('Pipeline Description').strip())

    def _create_submission_xml(self, files_to_submit, action, project_row, eload):
        root = Element('SUBMISSION_SET')
        submission_elemt = add_element(root, 'SUBMISSION',
                                       alias=eload,
                                       center_name=project_row.get('Center'))
        actions_elemt = add_element(submission_elemt, 'ACTIONS')
        for file_dict in files_to_submit:
            action_elemt = add_element(actions_elemt, 'ACTION')
            add_element(action_elemt, action.upper(),  # action should be ADD or MODIFY
                        source=os.path.basename(file_dict['file_name']),
                        schema=file_dict['schema'])

        if 'Hold Date' in project_row and project_row.get('Hold Date'):
            hold_date = project_row.get('Hold Date')
        else:
            hold_date = today() + timedelta(days=3)

        self.hold_date = hold_date

        action_elemt = add_element(actions_elemt, 'ACTION')
        add_element(action_elemt, 'HOLD', HoldUntilDate=hold_date.strftime('%Y-%m-%d'))
        return root

    def _create_submission_single_xml(self,  action, project_row, eload):
        root = Element('SUBMISSION_SET')
        submission_elemt = add_element(root, 'SUBMISSION',
                                       alias=eload,
                                       center_name=project_row.get('Center'))
        actions_elemt = add_element(submission_elemt, 'ACTIONS')
        action_elemt = add_element(actions_elemt, 'ACTION')
        # action should be ADD or MODIFY
        add_element(action_elemt, action.upper())
        if 'Hold Date' in project_row and project_row.get('Hold Date'):
            hold_date = project_row.get('Hold Date')
        else:
            hold_date = today() + timedelta(days=3)

        self.hold_date = hold_date
        action_elemt = add_element(actions_elemt, 'ACTION')
        add_element(action_elemt, 'HOLD', HoldUntilDate=hold_date.strftime('%Y-%m-%d'))
        return root

    @staticmethod
    def write_xml_to_file(xml_element, output_file):
        xml_element.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
        etree = ElementTree(xml_element)
        with open(output_file, 'bw') as open_file:
            open_file.write(prettify(etree))

    def create_submission_files(self, eload):
        files_to_submit = []
        if not self.is_existing_project:
            files_to_submit.append(
                {'file_name': os.path.basename(self.project_file), 'schema': 'project'}
            )
            projects_elemt = self._create_project_xml()
            self.write_xml_to_file(projects_elemt, self.project_file)
            project_file = self.project_file
        else:
            project_file = None

        analysis_elemt = self._create_analysis_xml()
        self.write_xml_to_file(analysis_elemt, self.analysis_file)
        files_to_submit.append(
            {'file_name': os.path.basename(self.analysis_file), 'schema': 'analysis'}
        )

        action = 'ADD'
        submission_elemt = self._create_submission_xml(files_to_submit, action, self.reader.project, eload)
        self.write_xml_to_file(submission_elemt, self.submission_file)

        return self.submission_file, project_file, self.analysis_file

    def create_single_submission_file(self, eload):
        root = Element('WEBIN')
        # Submission ELEMENT
        action = 'ADD'
        submissions_elemt = self._create_submission_single_xml(action, self.reader.project, eload)
        root.append(submissions_elemt)

        # Project ELEMENT
        if not self.is_existing_project:
            projects_elemt = self._create_project_xml()
            root.append(projects_elemt)

        # Analysis ELEMENT
        analysis_elemt = self._create_analysis_xml()
        root.append(analysis_elemt)

        self.write_xml_to_file(root, self.single_submission_file)

        return self.single_submission_file
