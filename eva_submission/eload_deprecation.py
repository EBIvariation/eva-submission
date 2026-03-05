# Copyright 2026 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import csv
import glob
import gzip
import os
import random
import shutil
import string
import subprocess

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle, resolve_variant_warehouse_db_name
from ebi_eva_internal_pyutils.spring_properties import SpringPropertiesGenerator
from sqlalchemy import select

from eva_submission import NEXTFLOW_DIR
from eva_submission.eload_utils import get_nextflow_config_flag, open_gzip_if_required
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import Analysis, File, Project, ProjectEvaSubmission, Taxonomy

DEPRECATE_ACCESSION = 'deprecate_variants'
DROP_STUDY = 'drop_study'
MARK_STUDY_INACTIVE = 'mark_inactive'
all_tasks = [DEPRECATE_ACCESSION, DROP_STUDY, MARK_STUDY_INACTIVE]


class StudyDeprecation(AppLogger):
    config_section = 'deprecation'
    all_tasks = all_tasks
    nextflow_complete_value = '<complete>'

    def __init__(self, project_accession, output_dir, nextflow_config=None):
        self.project_accession = project_accession
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        self.nextflow_config = nextflow_config
        self.private_settings_file = cfg['maven']['settings_file']
        self.maven_profile = cfg['maven']['environment']
        self.properties_generator = SpringPropertiesGenerator(self.maven_profile, self.private_settings_file)
        self.loader = EvaProjectLoader()
        self._config_file = os.path.join(output_dir, 'deprecate_study_config.yaml')
        self._config = {}
        if os.path.exists(self._config_file):
            with open(self._config_file) as f:
                self._config = yaml.safe_load(f) or {}

    def _save_config(self):
        with open(self._config_file, 'w') as f:
            yaml.safe_dump(self._config, f)

    def _get_cfg(self, *keys):
        obj = self._config
        for key in keys:
            if not isinstance(obj, dict) or key not in obj:
                return None
            obj = obj[key]
        return obj

    def _set_cfg(self, *keys, value):
        obj = self._config
        for key in keys[:-1]:
            obj = obj.setdefault(key, {})
        obj[keys[-1]] = value
        self._save_config()

    def get_assemblies_and_db_names(self):
        """
        Query EVAPRO for all (assembly_accession, db_name) pairs associated with the project.
        Returns a list of (assembly_accession, db_name) tuples.
        """
        query = (
            select(Analysis.vcf_reference_accession).distinct()
            .join(Analysis.projects)
            .where(Project.project_accession == self.project_accession)
        )
        assembly_accessions = [
            row[0] for row in self.loader.eva_session.execute(query).fetchall()
            if row[0]
        ]
        # Get taxonomy for the project
        taxonomy_query = (
            select(Taxonomy.taxonomy_id)
            .join(Taxonomy.projects)
            .where(Project.project_accession == self.project_accession)
        )
        taxonomy_ids = [row[0] for row in self.loader.eva_session.execute(taxonomy_query).fetchall()]
        if not taxonomy_ids:
            raise ValueError(f'No taxonomy found for project {self.project_accession} in EVAPRO')
        taxonomy_id = taxonomy_ids[0]

        with get_metadata_connection_handle(self.maven_profile, self.private_settings_file) as conn:
            results = []
            for assembly_accession in assembly_accessions:
                db_name = resolve_variant_warehouse_db_name(conn, assembly_accession, taxonomy_id,
                                                            ncbi_api_key=cfg.get('eutils_api_key'))
                if not db_name:
                    raise ValueError(f'Could not resolve db_name for assembly {assembly_accession} '
                                     f'and taxonomy {taxonomy_id}')
                results.append((assembly_accession, db_name))
        return results

    def get_accession_reports_for_project(self):
        """
        Resolve accession report files (*.accessioned.vcf.gz) for the project.

        1. Query project_eva_submission for eload_id(s) linked to this project.
        2. Query EVAPRO for (filename, assembly) pairs via Project → Analysis → File.
        3. For each eload, glob 60_eva_public/*accessioned.vcf.gz and match to assembly
           using the convention: {base}.vcf.gz → {base}.accessioned.vcf.gz

        Returns dict: assembly_accession -> list of accession report file paths.
        Raises ValueError if no eload is found for the project.
        """
        # Step 1: Get eload_id(s)
        eload_query = (
            select(ProjectEvaSubmission.eload_id).distinct()
            .where(ProjectEvaSubmission.project_accession == self.project_accession)
            .where(ProjectEvaSubmission.eload_id.isnot(None))
        )
        eload_ids = [row[0] for row in self.loader.eva_session.execute(eload_query).fetchall()]
        if not eload_ids:
            raise ValueError(f'No eload ID found for project {self.project_accession} in EVAPRO')

        # Step 2: Build {original_base: assembly} from EVAPRO (Project → Analysis → File)
        files_query = (
            select(File.filename, Analysis.vcf_reference_accession).distinct()
            .join(File.analyses)
            .join(Analysis.projects)
            .where(Project.project_accession == self.project_accession)
        )
        base_to_assembly = {}
        for filename, assembly in self.loader.eva_session.execute(files_query).fetchall():
            if filename.endswith('.vcf.gz'):
                base = filename[:-len('.vcf.gz')]
            elif filename.endswith('.vcf'):
                base = filename[:-len('.vcf')]
            else:
                continue
            base_to_assembly[base] = assembly

        # Step 3: Glob 60_eva_public in each eload, match report to assembly
        assembly_to_reports = {}
        suffix = '.accessioned.vcf.gz'
        for eload_id in eload_ids:
            eload_dir = os.path.join(cfg['eloads_dir'], f'ELOAD_{eload_id}')
            for report_path in glob.glob(os.path.join(eload_dir, '60_eva_public', f'*{suffix}')):
                report_basename = os.path.basename(report_path)
                original_base = report_basename[:-len(suffix)]
                assembly = base_to_assembly.get(original_base)
                if assembly is None:
                    self.warning(f'Could not match accession report {report_path} to any file in EVAPRO')
                    continue
                assembly_to_reports.setdefault(assembly, []).append(report_path)

        return assembly_to_reports

    def extract_ss_ids_from_accession_reports(self, accession_report_paths, output_path):
        """
        Extract SS IDs from one or more VCF accession reports into a flat text file.
        """
        with open(output_path, 'w') as id_out:
            for report_path in accession_report_paths:
                with open_gzip_if_required(report_path) as vcf_in:
                    for line in vcf_in:
                        if line.startswith('#'):
                            continue
                        fields = line.split('\t')
                        if len(fields) < 3:
                            continue
                        id_field = fields[2].strip()
                        if id_field.startswith('ss'):
                            id_out.write(id_field[2:] + '\n')  # ss1234567 → 1234567
        return output_path

    def create_deprecation_properties(self, deprecation_suffix, deprecation_reason):
        """
        Generate a generic Spring properties file for the deprecation pipeline.
        """
        properties = self.properties_generator._format(
            self.properties_generator._common_accessioning_clustering_properties(
                assembly_accession=None,
                read_preference='secondaryPreferred',
                chunk_size=100
            ),
            {
                'spring.batch.job.names': 'DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_JOB',
                'parameters.deprecationIdSuffix': deprecation_suffix,
                'parameters.deprecationReason': deprecation_reason,
            }
        )
        output_path = os.path.join(self.output_dir, f'variant_deprecation.properties')
        with open(output_path, 'w') as f:
            f.write(properties)
        return output_path

    def create_drop_study_properties(self):
        """
        Generate a Spring properties file for the drop-study-job.
        The db_name and study id are supplied on the Nextflow command line.
        Returns the path to the written properties file.
        """
        properties = self.properties_generator.get_accession_import_properties(
            opencga_path=cfg['opencga_path']
        )
        output_path = os.path.join(self.output_dir, f'{DROP_STUDY}.properties')
        with open(output_path, 'w') as f:
            f.write(properties)
        return output_path

    def create_deprecation_csv(self, assembly_db_pairs, variant_id_files):
        """
        Write the CSV file consumed by the Nextflow deprecate_study workflow.
        Returns the path to the written CSV file.
        """
        csv_path = os.path.join(self.output_dir, 'source_deprecations.csv')
        with open(csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['assembly_accession', 'variant_id_file', 'db_name'])
            for assembly_accession, db_name in assembly_db_pairs:
                variant_id_file = variant_id_files.get(assembly_accession)
                if not variant_id_file:
                    raise ValueError(f'No variant_id_file provided for assembly {assembly_accession}')
                writer.writerow([assembly_accession, variant_id_file, db_name])
        return csv_path

    def run_deprecate_study_workflow(self, resume, tasks, source_csv_path, deprecation_suffix, deprecation_reason):
        """Run the deprecate_study Nextflow workflow for the relevant tasks."""
        nextflow_tasks = [t for t in tasks if t in [DEPRECATE_ACCESSION, DROP_STUDY]]
        if not nextflow_tasks:
            return

        drop_study_props = self.create_drop_study_properties()
        deprecation_props = self.create_deprecation_properties(deprecation_suffix, deprecation_reason)
        params = {
            'valid_deprecations': os.path.join(self.output_dir, 'valid_deprecations.csv'),
            'project_accession': self.project_accession,
            'drop_study_props': drop_study_props,
            'deprecation_props': deprecation_props,
            'logs_dir': self.output_dir,
            'jar': cfg['jar'],
            'tasks': nextflow_tasks,
            'source_deprecations': source_csv_path
        }
        self.run_nextflow('deprecate_study', params, resume, nextflow_tasks)

    def run_nextflow(self, workflow_name, params, resume, tasks):
        """
        Runs a Nextflow workflow using the provided parameters.
        Creates a work directory and removes it on success; preserves it on failure for resume.
        Task completion state is tracked in the local config file.
        """
        work_dir = None
        if resume:
            completed_tasks = [
                task for task in tasks
                if self._get_cfg(self.config_section, workflow_name, 'nextflow_dir', task) == self.nextflow_complete_value
            ]
            for task in completed_tasks:
                self.info(f'Task {task} already completed, skipping.')
            for task in completed_tasks:
                tasks = [t for t in tasks if t != task]
            if not tasks:
                self.info('No more tasks to perform: skipping Nextflow run.')
                return
            work_dirs = [
                self._get_cfg(self.config_section, workflow_name, 'nextflow_dir', task)
                for task in tasks
            ]
            work_dirs = set(w for w in work_dirs if w and os.path.exists(w))
            if len(work_dirs) == 1:
                work_dir = work_dirs.pop()
            else:
                self.warning(f'Work directory for {workflow_name} not found, will start from scratch.')
                work_dir = None

        if not resume or not work_dir:
            random_string = ''.join(random.choice(string.ascii_letters) for _ in range(6))
            work_dir = os.path.join(self.output_dir, f'nextflow_output_{random_string}')
            os.makedirs(work_dir)
            for task in tasks:
                self._set_cfg(self.config_section, workflow_name, 'nextflow_dir', task, value=work_dir)

        params_file = os.path.join(self.output_dir, f'{workflow_name}_params.yaml')
        with open(params_file, 'w') as f:
            yaml.safe_dump(params, f)
        nextflow_script = os.path.join(NEXTFLOW_DIR, f'{workflow_name}.nf')

        try:
            command_utils.run_command_with_output(
                f'Nextflow {workflow_name} process',
                ' '.join((
                    'export NXF_OPTS="-Xms1g -Xmx8g"; ',
                    cfg['executable']['nextflow'], nextflow_script,
                    '-params-file', params_file,
                    '-work-dir', work_dir,
                    '-resume' if resume else '',
                    get_nextflow_config_flag(self.nextflow_config)
                ))
            )
            shutil.rmtree(work_dir)
            for task in tasks:
                self._set_cfg(self.config_section, workflow_name, 'nextflow_dir', task,
                              value=self.nextflow_complete_value)
            return tasks
        except subprocess.CalledProcessError as e:
            error_msg = (f'Nextflow {workflow_name} pipeline failed: results might not be complete. '
                         f'See Nextflow logs in {self.output_dir}/.nextflow.log for more details.')
            self.error(error_msg)
            raise e

    def mark_project_inactive_in_evapro(self):
        """Update EVAPRO: set eva_status=0 on project and hidden_in_eva=1 on all linked analyses."""
        self.loader.mark_project_inactive(self.project_accession)
        self.loader.mark_analyses_hidden(self.project_accession)
        self.loader.refresh_study_browser()

    def deprecate(self, assembly_accession_reports, deprecation_suffix, deprecation_reason,
                  tasks=None, resume=False):
        """
        Main entry point for deprecating a study.

        :param assembly_accession_reports: dict mapping assembly_accession ->
            list of *.accessioned.vcf.gz paths (the accession reports)
        :param deprecation_suffix: suffix appended to the deprecation operation ID
        :param deprecation_reason: human-readable reason for the deprecation
        :param tasks: list of tasks to perform; defaults to all_tasks
        :param resume: whether to resume an existing Nextflow run
        """
        if tasks is None:
            tasks = list(self.all_tasks)

        nextflow_tasks = [t for t in tasks if t in [DEPRECATE_ACCESSION, DROP_STUDY]]

        if nextflow_tasks:
            # Extract SS IDs from accession reports → variant_id_files_mapping
            variant_id_files_mapping = {}
            for assembly, report_paths in assembly_accession_reports.items():
                variant_id_file = os.path.join(self.output_dir, f'{assembly}_variant_ids.txt')
                self.extract_ss_ids_from_accession_reports(report_paths, variant_id_file)
                variant_id_files_mapping[assembly] = variant_id_file

            assembly_db_pairs = self.get_assemblies_and_db_names()
            source_csv_path = self.create_deprecation_csv(assembly_db_pairs, variant_id_files_mapping)
            self.run_deprecate_study_workflow(resume, tasks, source_csv_path, deprecation_suffix, deprecation_reason)

        if MARK_STUDY_INACTIVE in tasks:
            self.mark_project_inactive_in_evapro()