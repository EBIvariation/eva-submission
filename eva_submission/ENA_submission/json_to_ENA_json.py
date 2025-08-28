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

    def create_single_submission_file(self):
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
            'analyses': ena_analysis_json_obj
        })
        if ena_projects_json_obj:
            ena_json_data['projects'] = [ena_projects_json_obj]
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
            "alias": project_alias,
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
        if project_accession:
            ena_project_obj["accession"] = project_accession

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
            alias = project.get('alias', '')
            title = project.get('title', '')
            if re.match(r'^PRJ(EB|NA)', alias):
                return {"accession": alias}
            elif re.match(r'^PRJ(EB|NA)', title):
                return {"accession": title}
            elif alias:
                return {"alias": alias}
            elif title:
                return {"alias": title}
            return {}

        def get_samples(samples):
            return [{
                "accession": sample.get('bioSampleAccession') or sample.get('accession') ,
                "alias": sample.get('sampleInVCF')
            } for sample in samples]

        def get_runs(analysis):
            return analysis.get('runAccessions', [])

        def get_assemblies(analysis):
            reference_genome = analysis.get('referenceGenome', '').strip()
            if is_single_insdc_sequence(reference_genome):
                return [{"assembly": {"accession": reference_genome}}]
            else:
                assembly = {}
                if reference_genome.split(':')[0] in ['file', 'http', 'ftp']:
                    assembly["custom"] = {"urlLink": reference_genome}
                else:
                    assembly["standard"] = reference_genome
                return [{"assembly": assembly}]

        def get_experiments(analysis):
            return [experiment.strip().capitalize()
                    for experiment in analysis.get('experimentType', '').split(':')]

        def get_analyses_attributes(analysis):
            analysis_attributes = []
            if analysis.get('software'):
                analysis_attributes.append({"tag": "SOFTWARE", "value": analysis['software'].strip()})
            if analysis.get('platform'):
                analysis_attributes.append({"tag": "PLATFORM", "value": analysis['platform'].strip()})
            if str(analysis.get('imputation', '')).strip() == '1':
                analysis_attributes.append({"tag": "IMPUTATION", "value": "1"})

            return analysis_attributes

        def get_file_objs(files):
            def _file_type(fn):
                if is_vcf_file(fn):
                    return {'fileType': 'vcf'}
                elif fn.endswith('tbi'):
                    return {'fileType': 'tabix'}
                elif fn.endswith('csi'):
                    return {'fileType': 'csi'}
                return {}

            return [
                {
                    'fileName': file['fileName'],
                    **(_file_type(file.get('fileName'))),
                    **({"checksumMethod": 'MD5'} if 'md5' in file else {}),
                    **({"checksum": file.get('md5')} if 'md5' in file else {}),
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
            "alias": analysis['analysisAlias'],
            "title": analysis['analysisTitle'],
            "description": analysis['description'],
            "centreName": get_centre(analysis, project),
            "study": get_study_attr(project),
            "samples": get_samples(samples),
            "runs": get_runs(analysis),
            "analysisType": "SEQUENCE_VARIATION",
            "assemblies": get_assemblies(analysis),
            # "experiments": get_experiments(analysis),
            "attributes": get_analyses_attributes(analysis),
            "files": get_file_objs(files),
            "links": get_analysis_links(analysis),
        }

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
        centreName = project_data.get('centre')

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
            "centerName": centreName,
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
