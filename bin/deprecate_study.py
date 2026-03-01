#!/usr/bin/env python

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

import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.eload_deprecation import StudyDeprecation
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Deprecate a study from EVA (variants, MongoDB, EVAPRO)')
    argparse.add_argument('--project_accession', required=True,
                          help='Project accession to deprecate (e.g. PRJEB12345)')
    argparse.add_argument('--assemblies_accession_reports', required=False, nargs='+',
                          help='Mapping of assembly_accession=accession_report.vcf.gz. '
                               'If omitted, derived automatically from EVAPRO. '
                               'e.g. GCA_000001405.2=/path/to/file.accessioned.vcf.gz')
    argparse.add_argument('--deprecation_suffix', required=True,
                          help='Suffix used to build the deprecation operation ID')
    argparse.add_argument('--deprecation_reason', required=True,
                          help='Human-readable reason for the deprecation')
    argparse.add_argument('--output_dir', required=False, default='.',
                          help='Working directory for properties files, CSVs, and Nextflow output')
    argparse.add_argument('--tasks', required=False, type=str, nargs='+',
                          default=StudyDeprecation.all_tasks,
                          choices=StudyDeprecation.all_tasks,
                          help='Task(s) to perform: deprecate_variants, drop_study, mark_inactive')
    argparse.add_argument('--resume', action='store_true', default=False,
                          help='Resume an existing Nextflow run')
    argparse.add_argument('--nextflow_config', type=str, required=False,
                          help='Path to a Nextflow config file to apply to the workflow')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Enable debug logging')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config from the default location
    load_config()

    deprecation = StudyDeprecation(
        project_accession=args.project_accession,
        output_dir=args.output_dir,
        nextflow_config=args.nextflow_config,
    )

    if args.assemblies_accession_reports:
        assembly_accession_reports = {}
        for item in args.assemblies_accession_reports:
            if '=' not in item:
                raise ValueError(
                    f'Expected format assembly_accession=report_file, got: {item!r}'
                )
            assembly, report_file = item.split('=', 1)
            assembly_accession_reports[assembly] = [report_file]
    else:
        assembly_accession_reports = deprecation.get_accession_reports_for_project()

    deprecation.deprecate(
        assembly_accession_reports=assembly_accession_reports,
        deprecation_suffix=args.deprecation_suffix,
        deprecation_reason=args.deprecation_reason,
        tasks=args.tasks,
        resume=args.resume,
    )


if __name__ == '__main__':
    main()
