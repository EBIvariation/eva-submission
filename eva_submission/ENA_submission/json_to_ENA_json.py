import json
import os.path
import re
from collections import defaultdict
from datetime import datetime, timedelta
from functools import cached_property

from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl

from eva_submission.eload_utils import check_project_format, check_existing_project_in_ena, is_single_insdc_sequence, \
    is_vcf_file


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

    @cached_property
    def is_existing_project(self):
        return self.existing_project is not None

    @cached_property
    def existing_project(self):
        project_accession = self.eva_json_data.get('project', {}).get('projectAccession')
        if project_accession:
            assert check_project_format(project_accession), f'{project_accession} does not match a project accession pattern'
            assert check_existing_project_in_ena(project_accession), f'{project_accession} does not seem to exist or is not public'
        return project_accession

    def create_single_submission_file(self):
        ena_json_data = {}

        ena_projects_json_obj = (
            [self._create_ena_project_json_obj(self.eva_json_data['project'])]
            if not self.is_existing_project else []
        )

        ena_analysis_json_obj = self._create_ena_analysis_json_obj()
        ena_submission_json_obj = self._create_ena_submission_json_obj(
            self.eva_json_data['project'], self.submission_id
        )

        ena_json_data.update({
            'submission': ena_submission_json_obj,
            'analyses': ena_analysis_json_obj
        })
        if ena_projects_json_obj:
            ena_json_data['projects'] = ena_projects_json_obj
        self.write_to_json(ena_json_data, self.output_ena_json_file)

        return self.output_ena_json_file

    def write_to_json(self, json_data, json_file):
        with open(json_file, 'w') as open_file:
            json.dump(json_data, open_file, indent=4)

    @property
    def _project_alias(self):
        return f"{self.existing_project}_{self.submission_id}" if self.is_existing_project else self.submission_id

    def _get_file_obs(self, file):
        def _file_type(fn):
            if is_vcf_file(fn):
                return {'fileType': 'vcf'}
            elif fn.endswith('tbi'):
                return {'fileType': 'tabix'}
            elif fn.endswith('csi'):
                return {'fileType': 'csi'}
            return {}

        return {
            'fileName': file['fileName'],
            **(_file_type(file.get('fileName'))),
            **({"checksumMethod": 'MD5'} if 'md5' in file else {}),
            **({"checksum": file.get('md5')} if 'md5' in file else {}),
        }

    def _get_file_obs(self, file):
        def _file_type(fn):
            if is_vcf_file(fn):
                return {'fileType': 'vcf'}
            elif fn.endswith('tbi'):
                return {'fileType': 'tabix'}
            elif fn.endswith('csi'):
                return {'fileType': 'csi'}
            return {}

        return {
            'fileName': file['fileName'],
            **(_file_type(file.get('fileName'))),
            **({"checksumMethod": 'MD5'} if 'md5' in file else {}),
            **({"checksum": file.get('md5')} if 'md5' in file else {}),
        }


    def _create_ena_project_json_obj(self, project_data):
        project_title = project_data.get("title", "Unknown Title")
        project_description = project_data.get("description", "No description provided")
        project_centre_name = project_data.get("centre", "Unknown Centre")

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
            "alias": self._project_alias,
            "title": project_title,
            "description": project_description,
            "centreName": project_centre_name,
            "publicationLinks": publication_links,
            "sequencingProject": {},
            "organism": {
                "taxonId": tax_id,
                "scientificName": scientific_name,
            },
            "relatedProjects": related_projects,
            "projectLinks": project_links,
        }
        if self.is_existing_project:
            ena_project_obj["accession"] = self.existing_project

        return ena_project_obj

    def _create_ena_analysis_json_obj(self, ):
        samples_per_analysis = self._samples_per_analysis(self.eva_json_data.get('sample', []))
        files_per_analysis = self._files_per_analysis(self.eva_json_data.get('files', []))
        analyses_json = []
        for analysis in self.eva_json_data['analysis']:
            samples = samples_per_analysis[analysis.get('analysisAlias')]
            files = files_per_analysis[analysis.get('analysisAlias')]

            analyses_json.append(self._add_analysis(analysis, samples, files, self.eva_json_data['project']))
        return analyses_json

    def _add_analysis(self, analysis, samples, files, project):
        def get_centre(analysis, project):
            return analysis.get('centre') or project.get('centre')

        def get_study_attr(project):
            if self.is_existing_project:
                return {"accession": self.existing_project}
            elif self._project_alias:
                return {"alias": self._project_alias}
            return {}

        def get_samples(samples):
            return [{
                "accession": sample.get('bioSampleAccession') or sample.get('accession') ,
                "alias": sample.get('sampleInVCF')
            } for sample in samples]

        def get_runs(analysis):
            return [{'accession': run} for run in analysis.get('runAccessions', [])]

        def get_assemblies(analysis):
            reference_genome = analysis.get('referenceGenome', '').strip()
            if is_single_insdc_sequence(reference_genome):
                return [{"sequence": {"accession": reference_genome}}]
            else:
                assembly = {}
                if reference_genome.split(':')[0] in ['file', 'http', 'ftp']:
                    assembly["custom"] = {"urlLink": reference_genome}
                else:
                    assembly["accession"] = reference_genome
                    assembly["refname"] = reference_genome
                return [{"assembly": assembly}]

        def get_experiment_types(analysis):
            return [experiment.strip().capitalize()
                    for experiment in analysis.get('experimentType', '').split(':')]

        def get_analyses_attributes(analysis):
            analysis_attributes = []
            if analysis.get('software'):
                analysis_attributes.extend([
                    {"tag": "SOFTWARE", "value": a.strip()}
                    for a in analysis.get('software')
                ])
            if analysis.get('platform'):
                analysis_attributes.append({"tag": "PLATFORM", "value": analysis['platform'].strip()})
            if str(analysis.get('imputation', '')).strip() == '1':
                analysis_attributes.append({"tag": "IMPUTATION", "value": "1"})
            if analysis.get('experimentType', ''):
                analysis_attributes.extend([
                    {"tag": "EXPERIMENT_TYPE", "value": experiment}
                    for experiment in analysis.get('experimentType', '').split(':')
                ])
            return analysis_attributes

        def get_file_objs(files):
            return [self._get_file_obs(file) for file in files]

        def get_analysis_links(analysis):
            return [self.get_link(link) for link in analysis.get('links', [])]

        def get_attributes(analysis):
            if analysis.get('pipelineDescriptions'):
                return [{"tag": "pipelineDescription", "value": analysis['pipelineDescriptions'].strip()}]
            return None

        analysis_json_obj = {
            "alias": analysis['analysisAlias'],
            "title": analysis['analysisTitle'],
            "description": analysis['description'],
            "study": get_study_attr(project),
            "samples": get_samples(samples),
            "runs": get_runs(analysis),
            "analysisType": "SEQUENCE_VARIATION",
            "assemblies": get_assemblies(analysis),
            "experimentTypes": get_experiment_types(analysis),
            "attributes": get_analyses_attributes(analysis),
            "files": get_file_objs(files),
            "links": get_analysis_links(analysis),
        }
        if get_centre(analysis, project):
            analysis_json_obj["centreName"] = get_centre(analysis, project)

        attributes = get_attributes(analysis)
        if attributes:
            analysis_json_obj["attributes"] = attributes

        return analysis_json_obj

    def _samples_per_analysis(self, samples_data):
        samples_per_analysis = defaultdict(list)
        for sample in samples_data:
            for analysis_alias in sample.get('analysisAlias', []):
                samples_per_analysis[analysis_alias.strip()].append(sample)
        return samples_per_analysis

    def _files_per_analysis(self, files_data):
        files_per_analysis = defaultdict(list)
        for file in files_data:
            files_per_analysis[file.get('analysisAlias', '').strip()].append(file)
        return files_per_analysis

    def _create_ena_submission_json_obj(self, project_data, submission_id):
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
            "actions": [
                {"type": "ADD"},
                {"type": "HOLD", "holdUntilDate": self.hold_date},
            ],
        }
        if project_data.get('centre'):
            ena_submission_obj['centreName'] = project_data.get('centre')

        return ena_submission_obj

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
