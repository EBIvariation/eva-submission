import json
import os.path
import re
from collections import defaultdict
from datetime import datetime, timedelta
from functools import cached_property
from xml.etree.ElementTree import Element, ElementTree

from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.ENA_submission.json_to_ENA_json import EnaJsonConverter
from eva_submission.ENA_submission.xlsx_to_ENA_xml import add_element, add_links, add_attribute_elements, prettify
from eva_submission.eload_utils import check_project_format, check_existing_project_in_ena, is_single_insdc_sequence, \
    is_vcf_file


def today():
    return datetime.today()


class EnaJson2XmlConverter(EnaJsonConverter):

    def __init__(self, submission_id, input_eva_json, output_folder, output_file_name):
        super().__init__(submission_id, input_eva_json, output_folder, output_file_name)

        self.output_ena_json_file = None
        self.project_file = os.path.join(self.output_folder, self.output_file_name + '.Project.xml')
        self.analysis_file = os.path.join(self.output_folder, self.output_file_name + '.Analysis.xml')
        self.submission_file = os.path.join(self.output_folder, self.output_file_name + '.Submission.xml')
        self.single_submission_file = os.path.join(self.output_folder, self.output_file_name + '.SingleSubmission.xml')


    @cached_property
    def existing_project(self):
        project_accession = self.eva_json_data.get('project', {}).get('projectAccession')
        if project_accession:
            assert check_project_format(
                project_accession), f'{project_accession} does not match a project accession pattern'
            assert check_existing_project_in_ena(
                project_accession), f'{project_accession} does not seem to exist or is not public'
        return project_accession

    def _create_project_xml(self):
        """
        This function read the project row from the XLS parser then create and populate an XML element following ENA
        data model.
        :return: The top XML element
        """
        project_data = self.eva_json_data.get('project', {})
        project_title = project_data.get("title", "Unknown Title")
        project_description = project_data.get("description", "No description provided")
        project_centre_name = project_data.get("centre", "Unknown Centre")

        publication_links = [
            {"xrefLink": {"db": pub.split(":")[0], "id": pub.split(":")[1]}}
            for pub in project_data.get("publications", [])
            if ":" in pub
        ]
        root = Element('PROJECT_SET')

        project_elemt = add_element(root, 'PROJECT',
                                    alias=self._project_alias,
                                    center_name=project_centre_name)

        add_element(project_elemt, 'TITLE', project_title, content_required=True)
        add_element(project_elemt, 'DESCRIPTION', project_description, content_required=True)
        if "publications" in project_data and project_data.get("publications"):
            publications_elemt = add_element(project_elemt, 'PUBLICATIONS')
            publications = project_data.get("publications")
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


        sub_project_elemt = add_element(project_elemt, 'SUBMISSION_PROJECT')
        add_element(sub_project_elemt, 'SEQUENCING_PROJECT')
        tax_id = project_data.get("taxId", 0)
        scientific_name = get_scientific_name_from_ensembl(str(tax_id).strip())

        if tax_id:
            org_elemt = add_element(sub_project_elemt, 'ORGANISM')
            add_element(org_elemt, 'TAXON_ID', element_text=str(tax_id).strip())
            add_element(org_elemt, 'SCIENTIFIC_NAME', element_text=scientific_name)

        related_projects = []
        for key, field_name in [
            ("parentProject", "PARENT_PROJECT"),
            ("childProjects", "PEER_PROJECT"),
            ("peerProjects", "PEER_PROJECT"),
        ]:
            values = project_data.get(key, [])
            if isinstance(values, str):
                related_projects.append((field_name, values))
            elif isinstance(values, list):
                related_projects.extend([(field_name, value) for value in values])

        if related_projects:
            related_prjs_elemt = add_element(project_elemt, 'RELATED_PROJECTS')
            for field_name, related_project in related_projects:
                related_prj_elemt = add_element(related_prjs_elemt, 'RELATED_PROJECT')
                add_element(related_prj_elemt, field_name, accession=related_project)
        project_links = [self.get_link(link) for link in project_data.get("links", [])]

        if "links" in project_data and project_data.get("links", []):
            links_elemt = add_element(project_elemt, 'PROJECT_LINKS')
            project_links = project_data.get("links")
            add_links(links_elemt, project_links, link_type='PROJECT_LINK')
        return root

    def _create_analysis_xml(self):
        """
        This function reads the analysis rows from the XLS parser then create and populate an XML element following ENA data
        model.
        :return: The top XML element
        """
        root = Element('ANALYSIS_SET')
        samples_per_analysis = self._samples_per_analysis(self.eva_json_data.get('sample', []))
        files_per_analysis = self._files_per_analysis(self.eva_json_data.get('files', []))
        analyses_json = []
        for analysis_data in self.eva_json_data['analysis']:
            samples_data = samples_per_analysis[analysis_data.get('analysisAlias')]
            files_data = files_per_analysis[analysis_data.get('analysisAlias')]
            self._add_analysis(root, analysis_data, samples_data, files_data, self.eva_json_data['project'])
        return root

    def _add_analysis(self, root, analysis_data, samples_data, files_data, project_data):
        """
        Add an analysis element using information from the analysis row provided.
        The element is appended to the root element provided
        :param root: Top level element
        :param analysis_data: Dictionary representing one of the analysis
        :param project_data: Dictionary representing the project
        :param samples_data: List of Dictionary representing one of the sample
        :param files_data: list of Dictionary representing one of the file
        :return: None
        """
        analysis_elemt = add_element(root, 'ANALYSIS',
                                     alias=analysis_data.get('analysisAlias'),
                                     center_name=analysis_data.get('centre') or project_data.get('centre'))

        add_element(analysis_elemt, 'TITLE', element_text=analysis_data.get('analysisTitle'))
        add_element(analysis_elemt, 'DESCRIPTION', element_text=analysis_data.get('description'))

        # Add Project link
        study_attr = {}
        if self.is_existing_project:
            study_attr = {"accession": self.existing_project}
        elif self._project_alias:
            study_attr = {"refname": self._project_alias}
        add_element(analysis_elemt, 'STUDY_REF', **study_attr)

        # Add sample information for this analysis
        for sample_data in samples_data:
            add_element(analysis_elemt, 'SAMPLE_REF',
                        accession=sample_data.get('bioSampleAccession') or sample_data.get('accession'),
                        label=sample_data.get('sampleInVCF'))

        # Add run accessions
        if 'runAccessions' in analysis_data and analysis_data.get('runAccessions'):
            for run in analysis_data.get('runAccessions').split(','):
                add_element(analysis_elemt, 'RUN_REF', accession=run)

        # Add analysis type
        anal_type_elemt = add_element(analysis_elemt, 'ANALYSIS_TYPE')
        seq_var_elemt = add_element(anal_type_elemt, 'SEQUENCE_VARIATION')
        if is_single_insdc_sequence(analysis_data.get('referenceGenome').strip()):
            add_element(seq_var_elemt, 'SEQUENCE', accession=analysis_data.get('referenceGenome').strip())
        else:
            assembly_elemt = add_element(seq_var_elemt, 'ASSEMBLY')
            if analysis_data.get('referenceGenome').split(':')[0] in ['file', 'http', 'ftp']:
                custom_elemt = add_element(assembly_elemt, 'CUSTOM')
                url_link_elemt = add_element(custom_elemt, 'URL_LINK')
                add_element(url_link_elemt, 'URL', element_text=analysis_data.get('referenceGenome').strip())
            else:
                add_element(assembly_elemt, 'STANDARD', accession=analysis_data.get('referenceGenome').strip())
        experiments = analysis_data.get('experimentType').strip().split(':')
        for experiment in experiments:
            add_element(seq_var_elemt, 'EXPERIMENT_TYPE', element_text=experiment.lower().capitalize())
        add_element(seq_var_elemt, 'PROGRAM', element_text=analysis_data.get('software'), content_required=True)
        if 'platform' in analysis_data and analysis_data.get('platform'):
            platforms = analysis_data.get('platform').strip()
            add_element(seq_var_elemt, 'PLATFORM', element_text=platforms)
        if 'imputation' in analysis_data and analysis_data.get('imputation') \
                and str(analysis_data.get('imputation')).strip() == '1':
            add_element(seq_var_elemt, 'IMPUTATION', element_text='1')
        # JSON keys to XML attributes
        file_mapping = {
            'fileName':'filename',
            'fileType': 'filetype',
            'checksumMethod': 'checksum_method',
            'checksum': 'checksum'
        }
        files_elemt = add_element(analysis_elemt, 'FILES')
        for file_data in files_data:
            add_element(files_elemt, 'FILE',
                        **{
                            file_mapping[file_key]: file_val
                            for file_key, file_val in self._get_file_obs(file_data).items()
                        })

        if 'links' in analysis_data and analysis_data.get('links'):
            analysis_links_elemt = add_element(analysis_elemt, 'ANALYSIS_LINKS')
            analysis_links = analysis_data.get('links')
            add_links(analysis_links_elemt, analysis_links, link_type='ANALYSIS_LINK')

        analysis_attributes_elemt = add_attribute_elements(analysis_elemt, analysis_data, object_type='ANALYSIS')
        if 'pipelineDescriptions' in analysis_data and analysis_data.get('pipelineDescriptions'):
            analysis_attrib_elemt = add_element(analysis_attributes_elemt, 'ANALYSIS_ATTRIBUTE')
            add_element(analysis_attrib_elemt, 'TAG', element_text='Pipeline_Description')
            add_element(analysis_attrib_elemt, 'VALUE', element_text=analysis_data.get('pipelineDescriptions').strip())

    def _create_submission_xml(self, files_to_submit, action, project_data):
        root = Element('SUBMISSION_SET')
        if self.is_existing_project:
            submission_alias = self.existing_project + '_' + self.submission_id
        else:
            submission_alias = self.submission_id
        submission_elemt = add_element(root, 'SUBMISSION',
                                       alias=submission_alias,
                                       center_name=project_data.get('centre'))
        actions_elemt = add_element(submission_elemt, 'ACTIONS')
        for file_dict in files_to_submit:
            action_elemt = add_element(actions_elemt, 'ACTION')
            add_element(action_elemt, action.upper(),  # action should be ADD or MODIFY
                        source=os.path.basename(file_dict['file_name']),
                        schema=file_dict['schema'])
        if 'holdDate' in project_data and project_data.get('holdDate'):
            hold_date = project_data.get('holdDate')
        else:
            hold_date = today() + timedelta(days=3)

        self.hold_date = hold_date

        action_elemt = add_element(actions_elemt, 'ACTION')
        add_element(action_elemt, 'HOLD', HoldUntilDate=hold_date.strftime('%Y-%m-%d'))
        return root

    def _create_submission_single_xml(self,  action, project_data):
        root = Element('SUBMISSION_SET')
        if self.is_existing_project:
            submission_alias = self.existing_project + '_' + self.submission_id
        else:
            submission_alias = self.submission_id
        submission_elemt = add_element(root, 'SUBMISSION',
                                       alias=submission_alias,
                                       center_name=project_data.get('centre'))
        actions_elemt = add_element(submission_elemt, 'ACTIONS')
        action_elemt = add_element(actions_elemt, 'ACTION')
        # action should be ADD or MODIFY
        add_element(action_elemt, action.upper())
        if 'holdDate' in project_data and project_data.get('holdDate'):
            hold_date = project_data.get('holdDate')
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

    def create_single_submission_file(self):
        root = Element('WEBIN')
        # Submission ELEMENT
        action = 'ADD'
        submissions_elemt = self._create_submission_single_xml(action, self.eva_json_data.get('project', {}))
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
