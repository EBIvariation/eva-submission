import os
import re
from datetime import datetime, timedelta
from io import BytesIO
from xml.dom import minidom
from xml.etree.ElementTree import Element, ElementTree

from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


def today():
    return datetime.today()


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


def new_analysis():
    elem = Element('ANALYSIS_SET')
    elem.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    return elem


def new_sample():
    elem = Element('SAMPLE_SET')
    elem.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    return elem


def new_project():
    elem = Element('PROJECT_SET')
    elem.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    return elem


def new_submission():
    elem = Element('SUBMISSION_SET')
    elem.attrib['xmlns:xsi'] = 'http://www.w3.org/2001/XMLSchema-instance'
    return elem


def add_project(root, project_row):
    """
    This function take a project row from the XLS parser and populate
    :param root:
    :param project_row:
    :return:
    """
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
        add_links(links_elemt, project_links)

    # TODO: Is this still relevant because it is not documented in the metadata template
    add_attribute_elements(project_elemt, project_row, object_type='PROJECT')
    return root


def add_links(links_elemt, links):
    for link in links:
        if re.match('^(ftp:|http:|file:|https:)', link):
            # TODO: requirement in the format
            url, label = link.split('|')
            url_link_elemt = add_element(links_elemt, 'URL_LINK')
            add_element(url_link_elemt, 'LABEL', element_text=label)
            add_element(url_link_elemt, 'URL', element_text=url)
        else:
            xlink_elemt = add_element(links_elemt, 'XREF_LINK')
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


def add_analysis(root, analysis_row, project_row, sample_rows, file_rows):
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
    if 'Imputation' in analysis_row and analysis_row.get('inputation') \
            and str(analysis_row.get('inputation').strip()) == '1':
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
        add_links(analysis_links_elemt, analysis_links)

    # TODO: Is this still relevant because it is not documented in the metadata template
    analysis_attributes_elemt = add_attribute_elements(analysis_elemt, analysis_row, object_type='ANALYSIS')
    if 'Pipeline Description' in analysis_row and analysis_row.get('Pipeline Description'):
        analysis_attrib_elemt = add_element(analysis_attributes_elemt, 'ANALYSIS_ATTRIBUTE')
        add_element(analysis_attrib_elemt, 'TAG', element_text='Pipeline_Description')
        add_element(analysis_attrib_elemt, 'VALUE', element_text=analysis_row.get('Pipeline Description').strip())


def add_submission(root, files_to_submit, action, project_row):
    submission_elemt = add_element(root, 'SUBMISSION',
                                   alias=project_row.get('Project Alias').strip(),
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

    action_elemt = add_element(actions_elemt, 'ACTION')
    add_element(action_elemt, 'HOLD', HoldUntilDate=hold_date.strftime('%Y-%m-%d'))


def prettify(etree):
    """Return a pretty-printed XML string for the ElementTree. """
    outfile = BytesIO()
    etree.write(outfile, encoding='utf-8', xml_declaration=True)
    rough_string = outfile.getvalue()
    reparsed = minidom.parseString(rough_string)
    return reparsed.toprettyxml(indent='    ', encoding="utf-8")


def write_xml_to_file(xml_element, output_file):
    etree = ElementTree(xml_element)
    with open(output_file, 'bw') as open_file:
        open_file.write(prettify(etree))


def process_metadata_spreadsheet(metadata_file, output_folder, name, modify=False):
    reader = EvaXlsxReader(metadata_file)

    projects_elemt = new_project()
    add_project(projects_elemt, reader.project)
    analysis_elemt = new_analysis()
    for analysis_row in reader.analysis:
        sample_rows = reader.samples_per_analysis[analysis_row.get('Analysis Alias')]
        file_rows = reader.files_per_analysis[analysis_row.get('Analysis Alias')]
        add_analysis(analysis_elemt, analysis_row, reader.project, sample_rows, file_rows)

    project_file = os.path.join(output_folder, name + '.Project.xml')
    write_xml_to_file(projects_elemt, project_file)
    analysis_file = os.path.join(output_folder, name + '.Analysis.xml')
    write_xml_to_file(analysis_elemt, analysis_file)

    # default action to ADD
    if modify:
        action = 'MODIFY'
    else:
        action = 'ADD'

    files_to_submit = [
        {'file_name': os.path.basename(project_file), 'schema': 'project'},
        {'file_name': os.path.basename(analysis_file), 'schema': 'analysis'}
    ]
    submission_elemt = new_submission()
    add_submission(submission_elemt, files_to_submit, action, reader.project)
    submission_file = os.path.join(output_folder, name + '.Submission.xml')
    write_xml_to_file(submission_elemt, submission_file)
    return submission_file, project_file, analysis_file
