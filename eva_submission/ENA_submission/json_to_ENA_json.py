import json
import os.path
import re
from datetime import datetime, timedelta
from functools import cached_property

from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.eload_utils import check_project_format, check_existing_project_in_ena, is_single_insdc_sequence


def today():
    return datetime.today()


class EnaJsonConverter(AppLogger):

    def __init__(self, submission_id, input_eva_json, output_folder, output_file_name):
        self.submission_id = submission_id
        self.input_eva_json = input_eva_json
        self.output_folder = output_folder
        self.output_file_name = output_file_name
        with open(self.input_eva_json, 'r') as file:
            self.eva_json_data = json.load(file)

        self.output_ena_json_file = os.path.join(self.output_folder, self.output_file_name + '.json')

    def create_ena_submission(self):
        ena_json_data = {}

        if not self.is_existing_project:
            ena_projects_json_obj = [
                self._create_ena_project_json_obj(self.eva_json_data['project'], self.submission_id)]
        else:
            # TODO: if existing project - should the projects field in json be empty or should not exist at all
            ena_projects_json_obj = []

        ena_analysis_json_obj = self._create_ena_analysis_json_obj()
        ena_submission_json_obj = self._create_ena_submission_json_obj(self.eva_json_data['project'],
                                                                       self.submission_id)

        ena_json_data.update({
            'submission': ena_submission_json_obj,
            'projects': ena_projects_json_obj,
            'analysis': ena_analysis_json_obj
        })

        self.write_to_json(ena_json_data, self.output_ena_json_file)

        return self.output_ena_json_file

    def create_ena_submission(self):
        ena_json_data = {}

        ena_projects_json_obj = (
            [self._create_ena_project_json_obj(self.eva_json_data['project'], self.submission_id)]
            if not self.is_existing_project else []
        )

        ena_analysis_json_obj = self._create_ena_analysis_json_obj()
        ena_submission_json_obj = self._create_ena_submission_json_obj(
            self.eva_json_data['project'], self.submission_id
        )

        ena_json_data.update({
            'submission': ena_submission_json_obj,
            **({'projects': ena_projects_json_obj} if ena_projects_json_obj else {}),
            'analysis': ena_analysis_json_obj
        })

        self.write_to_json(ena_json_data, self.output_ena_json_file)

        return self.output_ena_json_file

    def write_to_json(self, json_data, json_file):
        with open(json_file, 'w') as file:
            json.dump(json_data, file, indent=4)

    def _create_ena_project_json_obj(self, project_data, submission_id):
        project_alias = (f"{self.existing_project}_{submission_id}" if self.is_existing_project else submission_id)

        project_accession = self.existing_project
        project_title = project_data.get("title", "Unknown Title")
        project_description = project_data.get("description", "No description provided")
        project_centre = project_data.get("centre", "Unknown Centre")

        publication_links = [
            {"xrefLink": {"db": pub.split(":")[0], "id": pub.split(":")[1]}}
            for pub in project_data.get("publications", [])
            if ":" in pub
        ]

        tax_id = project_data.get("taxId", 0)
        scientific_name = get_scientific_name_from_ensembl(str(tax_id).strip())

        related_projects = []
        for key, field_name in [
            ("parentProject", "parentProject"),
            ("childProjects", "childProject"),
            ("peerProjects", "peerProject"),
        ]:
            values = project_data.get(key, [])
            if isinstance(values, str):
                related_projects.append({field_name: values})
            elif isinstance(values, list):
                related_projects.extend([{field_name: value} for value in values])

        project_links = [self.get_link(link) for link in project_data.get("links", [])]

        ena_project_obj = {
            "accession": project_accession,
            "alias": project_alias,
            "title": project_title,
            "description": project_description,
            "centre": project_centre,
            "publicationLinks": publication_links,
            "sequencingProject": {},
            "organism": {
                "taxonId": tax_id,
                "scientificName": scientific_name,
            },
            "relatedProjects": related_projects,
            "projectLinks": project_links,
        }

        return ena_project_obj

    def _create_ena_analysis_json_obj(self):
        for analysis in self.eva_json_data['analysis']:
            samples = self._get_samples_for_analysis_alias(analysis, self.eva_json_data['samples'])
            files = self._get_files_for_analysis_alias(analysis, self.eva_json_data['files'])

            self._add_analysis(analysis, samples, files, self.eva_json_data['project'])

    def _add_analysis(self, analysis, samples, files, project):
        def get_centre(analysis, project):
            return analysis.get('centre') or project.get('centre')

        def get_study_attr(project):
            alias = project.get('alias', '')
            title = project.get('title', '')
            if re.match(r'^PRJ(EB|NA)', alias):
                return {"accession": alias}
            if re.match(r'^PRJ(EB|NA)', title):
                return {"accession": title}
            if alias:
                return {"refname": alias}
            if title:
                return {"refname": title}
            return {}

        def get_samples_refs(samples):
            # TODO: sample id ??
            return [{"sampleAccession": sample['bioSampleAccession'], "sampleId": sample['sampleId']} for sample in
                    samples]

        def get_run_refs(analysis):
            return analysis.get('runAccessions', [])

        def get_analysis_type(analysis):
            reference_genome = analysis.get('referenceGenome', '').strip()
            seq_asm = {}
            if is_single_insdc_sequence(reference_genome):
                seq_asm["sequence"] = reference_genome
            else:
                assembly = {}
                if reference_genome.split(':')[0] in ['file', 'http', 'ftp']:
                    assembly["custom"] = {"urlLink": reference_genome}
                else:
                    assembly["standard"] = reference_genome
                seq_asm["assembly"] = assembly

            experiment_types = [
                experiment.strip().capitalize()
                for experiment in analysis.get('experimentType', '').split(':')
            ]

            analysis_type = {
                "sequenceVariation": seq_asm,
                "experimentType": experiment_types,
            }

            # Add optional fields
            if analysis.get('software'):
                analysis_type['software'] = analysis['software'].strip()
            if analysis.get('platform'):
                analysis_type['platform'] = analysis['platform'].strip()
            if str(analysis.get('imputation', '')).strip() == '1':
                analysis_type['imputation'] = "1"

            return analysis_type

        def get_file_objs(files):
            return [
                {
                    'fileName': file['fileName'],
                    **({"fileType": file['fileType']} if 'fileType' in file else {}),
                    **({"checksumMethod": 'MD5'} if 'checksum' in file else {}),
                    **({"checksum": file['checksum']} if 'checksum' in file else {}),
                }
                for file in files
            ]

        def get_analysis_links(analysis):
            return [self.get_link(link) for link in analysis.get('links', [])]

        def get_attributes(analysis):
            if analysis.get('pipelineDescriptions'):
                return [{"tag": "pipelineDescription", "value": analysis['pipelineDescriptions'].strip()}]
            return None

        analysis_json_obj = {
            "title": analysis['analysisTitle'],
            "description": analysis['description'],
            "centre": get_centre(analysis, project),
            "studyRef": get_study_attr(project),
            "samplesRef": get_samples_refs(samples),
            "runRefs": get_run_refs(analysis),
            "analysisType": get_analysis_type(analysis),
            "files": get_file_objs(files),
            "analysisLinks": get_analysis_links(analysis),
        }

        attributes = get_attributes(analysis)
        if attributes:
            analysis_json_obj["attributes"] = attributes

        return analysis_json_obj

    def _get_samples_for_analysis_alias(self, samples, analysis_alias):
        pass

    def _get_files_for_analysis_alias(self, files, analysis_alias):
        pass

    def _create_ena_submission_json_obj(self, project_data, submission_id):
        centre = project_data.get('centre')

        submission_alias = (
            f"{self.existing_project}_{submission_id}" if self.is_existing_project else submission_id
        )

        hold_date = project_data.get('holdDate') or (today() + timedelta(days=3))
        # Make sure the hold date is in 'YYYY-MM-DD' format
        self.hold_date = (
            hold_date.strftime('%Y-%m-%d') if isinstance(hold_date, datetime) else hold_date
        )

        ena_submission_obj = {
            "alias": submission_alias,
            "centre": centre,
            "actions": [
                {"type": "ADD"},
                {"type": "HOLD", "holdUntilDate": self.hold_date},
            ],
        }

        return ena_submission_obj

    @cached_property
    def is_existing_project(self):
        return self.existing_project is not None

    @cached_property
    def existing_project(self):
        prj_alias = self.submission_id
        prj_title = self.eva_json_data['project']['title']
        if check_project_format(prj_alias) and check_existing_project_in_ena(prj_alias):
            return prj_alias
        elif check_project_format(prj_title) and check_existing_project_in_ena(prj_title):
            return prj_title
        return None

    def get_link(self, link):
        if re.match(r'^(ftp:|http:|file:|https:)', link):
            sp_link = link.split('|', maxsplit=1)
            url = sp_link[0]
            label = sp_link[1] if len(sp_link) > 1 else url
            return {
                "urlLink": {
                    "label": label,
                    "url": url
                }
            }
        else:
            link_parts = link.split(':', maxsplit=2)
            return {
                "xrefLink": {
                    "db": link_parts[0],
                    **({"id": link_parts[1]} if len(link_parts) > 1 else {}),
                    **({"label": link_parts[2]} if len(link_parts) > 2 else {})
                }
            }
