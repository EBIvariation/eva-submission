import urllib

import requests
from functools import cached_property
import xml.etree.ElementTree as ET

import oracledb
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.ena_utils import download_xml_from_ena


class ApiEnaProjectFinder:
    file_report_base_url = 'https://www.ebi.ac.uk/ena/portal/api/filereport'
    portal_search_base_url = 'https://www.ebi.ac.uk/ena/portal/api/search'

    def find_sample_aliases_per_accessions(self, accession_list):
        # Chunk the list in case it is too long
        results = []
        chunk_size = 100
        if accession_list:
            for i in range(0, len(accession_list), chunk_size):
                results.extend(self._find_sample_aliases_per_accessions(accession_list[i:i + chunk_size]))
        return results

    def _find_sample_aliases_per_accessions(self, accession_list):
        params = {
            'result': 'sample',
            'includeAccessionType': 'sample',
            'format': 'json',
            'fields': 'sample_accession,sample_alias',
            'includeAccessions': ','.join(accession_list)
        }
        url = self.portal_search_base_url
        response = requests.get(url, params=params)
        response.raise_for_status()
        json_data = response.json()
        samples = []
        for sample_data in json_data:
            samples.append((sample_data['sample_accession'], sample_data['sample_alias']))
        return samples

    def find_samples_from_analysis(self, accession):
        """
        This function leverage the filereport endpoint to retrieve the samples name to sample accession  dictionary
        organised by analysis.
        This function can be provided with an analysis or a project accession.
        returns a dictionary with key is tha analysis accession and value is another dictionary with key is sample
        accession and value the biosample name
        """
        url = self.file_report_base_url + f'?result=analysis&accession={accession}&format=json&fields=sample_accession,sample_alias'
        response = requests.get(url)
        response.raise_for_status()
        json_data = response.json()
        results_per_analysis = {}
        for analysis_data in json_data:
            sample_aliases = analysis_data.get('sample_alias').split(';')
            # the two lists are not necessarily in the same order so we look up the alias for each sample
            sample_accessions = [s for s in analysis_data.get('sample_accession').split(';') if s]
            results_per_analysis[analysis_data.get('analysis_accession')] = self.find_sample_aliases_per_accessions(sample_accessions)
        return results_per_analysis

    def find_samples_from_analysis_xml(self, analysis_accession):
        xml_root = download_xml_from_ena(f'https://www.ebi.ac.uk/ena/browser/api/xml/{analysis_accession}')
        xml_samples = xml_root.xpath('/ANALYSIS_SET/ANALYSIS/SAMPLE_REF')
        samples = []
        if xml_samples:
            for xml_sample in xml_samples:
                ena_accession = xml_sample.get('accession')
                sample_name = xml_sample.get('label')
                external_ids = xml_sample.xpath('IDENTIFIERS/EXTERNAL_ID')
                biosample_accession = None
                if external_ids:
                    for external_id in external_ids:
                        if external_id.get('namespace') == 'BioSample':
                            biosample_accession = external_id.text
                if not sample_name and biosample_accession:
                    sample_info = self.find_sample_aliases_per_accessions([biosample_accession])[0]
                    if sample_info:
                        biosample_accession, sample_name = sample_info
                if sample_name and biosample_accession:
                    samples.append((biosample_accession, sample_name))
        else:
            print('No samples found')
        return {analysis_accession: samples}


class OracleEnaProjectFinder:

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
        if not study_description:
            study_description = self._parse_project_description_from_xml(str(project_xml))
        # Project publication used to be parsed from the project XML. but there were none in the EVA Projects
        # project_publications ['PROJECT_XML/PUBLICATIONS/PUBLICATION/PUBLICATION_LINKS/PUBLICATION_LINK/[DB:ID]']
        return (
            study_id, project_accession, submission_id, center_name, project_alias, study_type, first_created,
            str(project_title), taxonomy_id, scientific_name, common_name, study_description
        )

    def find_parent_projects(self, project_accession):
        # link_type=2 == project
        # link_role=1 == hierarchical
        era_linked_project_query = (f"select to_id from era.ena_link "
                                    f"where TO_LINK_TYPE_ID=2 AND LINK_ROLE_ID=1 AND from_id='{project_accession}'")
        with self.era_cursor() as cursor:
            parent_projects = [to_id for to_id, in cursor.execute(era_linked_project_query)]
            return parent_projects

    def find_ena_submission_for_project(self, project_accession):
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
                alias, hold_date, action = self._parse_actions_and_alias_from_submission_xml(str(submission_xml))
                yield submission_id, alias, last_updated, hold_date, action

    def find_ena_submission_for_analysis(self, analysis_accession):
        era_submission_query = (
            "select sub.submission_id, sub.submission_xml, "
            "sub.last_updated UPDATED "
            "from era.submission sub "
            "left outer join era.ANALYSIS a on sub.submission_id=a.submission_id "
            f"where a.ANALYSIS_ID ='{analysis_accession}' and a.submission_id like 'ERA%'"
        )
        with self.era_cursor() as cursor:
            for results in cursor.execute(era_submission_query):
                (submission_id, submission_xml, last_updated) = results
                alias, hold_date, action = self._parse_actions_and_alias_from_submission_xml(str(submission_xml))
                yield submission_id, alias, last_updated, hold_date, action

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
        with self.era_cursor() as cursor:
            for results in cursor.execute(era_analysis_query):
                (
                    analysis_id, analysis_title, analysis_alias, analysis_type, center_name, first_created,
                    analysis_xml,  assembly, refname, custom
                ) = results
                analysis_description, experiment_types, platforms = self._parse_analysis_description_and_type_from_xml(str(analysis_xml))
                if analysis_type != 'SEQUENCE_VARIATION':
                    continue
                yield (
                    analysis_id, analysis_title, analysis_alias, analysis_description, analysis_type, center_name,
                    first_created, assembly, refname, custom, experiment_types, platforms
                )

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

    @cached_property
    def era_connection(self):
        era_cred = cfg.query('ena', 'ERA')
        return oracledb.connect(user=era_cred.get('username'),
                                password=era_cred.get('password'),
                                dsn=f"{era_cred.get('host')}:{era_cred.get('port')}/{era_cred.get('database')}")

    def era_cursor(self):
        return self.era_connection.cursor()

    @staticmethod
    def _parse_study_description_from_xml(study_xml):
        if study_xml and study_xml != 'None':
            root = ET.fromstring(study_xml)
            # Extract Study Description
            doc = root.find("./STUDY/DESCRIPTOR/STUDY_DESCRIPTION")
            if doc is not None:
                return doc.text
        return ''

    @staticmethod
    def _parse_project_description_from_xml(project_xml):
        if project_xml and project_xml != 'None':
            root = ET.fromstring(project_xml)
            # Extract Study Description
            doc = root.find("./PROJECT/DESCRIPTION")
            if doc is not None:
                return doc.text
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
                action = {"type": action_type}
                for attribute in  ["schema", "source"]:
                    if attribute in child.attrib:
                        action[attribute] = child.attrib[attribute]
                actions.append(action)
        # Prioritise actions -- Select project if present then analysis then something else
        project_action = [action for action in actions if action.get('schema') == 'project']
        analysis_action = [action for action in actions if action.get('schema') == 'analysis']
        other_actions = [action for action in actions if action.get('schema') not in ['project', 'analysis']]
        if project_action:
            action = project_action[0]
        elif analysis_action:
            action = analysis_action[0]
        elif other_actions:
            action = other_actions[0]
        else:
            action = None
        return submission_alias, hold_date, action

    def _parse_analysis_description_and_type_from_xml(self, analysis_xml):
        root = ET.fromstring(analysis_xml)
        description = root.find(".//DESCRIPTION").text if root.find(".//DESCRIPTION") is not None else None

        # Extract Analysis Type and associated elements
        analysis_type_element = root.find(".//ANALYSIS_TYPE")
        platforms = set()
        experiment_types = set()

        if analysis_type_element is not None:
            # Get the first child of ANALYSIS_TYPE (e.g., SEQUENCE_VARIATION)
            for child in analysis_type_element:
                # Extract associated elements
                for sub_element in child:
                    if sub_element.tag == 'EXPERIMENT_TYPE':
                        experiment_types.add(sub_element.text)
                    if sub_element.tag == 'PLATFORM':
                        platforms.add(sub_element.text)
        return description, experiment_types, platforms

