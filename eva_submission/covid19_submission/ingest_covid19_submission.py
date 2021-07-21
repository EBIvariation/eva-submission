# Copyright 2021 EMBL - European Bioinformatics Institute
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

import argparse
import glob
import inspect
import os
import sys

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils import config_utils
from ebi_eva_common_pyutils.nextflow import NextFlowPipeline, NextFlowProcess
from eva_submission.eload_ingestion import project_dirs
from typing import List

logger = logging_config.get_logger(__name__)


def get_python_process_command_string(python_program, args: dict, log_file: str):
    """
    Helper class to generate the command to run a Python program with command-line arguments
    """
    program_module_dir = os.path.dirname(inspect.getmodule(python_program).__file__)
    program_name = python_program.__name__.split(".")[-1]
    args_repr = " ".join([f"--{arg} {val}" for arg, val in args.items()])
    return f"bash -c \"export PYTHONPATH='{program_module_dir}' && " \
           f"{sys.executable} -m {program_name} {args_repr}\" 1>> {log_file} 2>&1"


def _get_vcf_filename_without_extension(vcf_file_name: str):
    return vcf_file_name.replace(".vcf.gz", "").replace(".vcf", "")


def get_vcf_preparation_stage(vcf_files: List[str], bcftools_binary: str) -> NextFlowPipeline:
    vcf_preparation_pipeline = NextFlowPipeline()
    for vcf_file_name in vcf_files:
        vcf_file_name_no_ext = _get_vcf_filename_without_extension(vcf_file_name)
        vcf_file_name_no_ext_and_path = os.path.basename(vcf_file_name_no_ext)
        vcf_preparation_command = f'bash -c "gunzip {vcf_file_name} && ' \
                                  f'{bcftools_binary} compress {vcf_file_name_no_ext}.vcf && ' \
                                  f'{bcftools_binary} index --csi {vcf_file_name_no_ext}.vcf"'
        vcf_preparation_process = NextFlowProcess(process_name=f"prepare_vcf_{vcf_file_name_no_ext_and_path}",
                                                  command_to_run=vcf_preparation_command)
        # All the VCF prep. processes can be run in parallel and hence don't need to be dependent on other processes
        vcf_preparation_pipeline.add_process_dependency(process=vcf_preparation_process, dependency=None)
    return vcf_preparation_pipeline


def get_validation_stage(vcf_files: List[str], vcf_validator_binary: str, assembly_checker_binary: str,
                         fasta_path: str, assembly_report_path: str,
                         validation_output_folder: str) -> NextFlowPipeline:
    vcf_validation_report_dir = f"{validation_output_folder}/vcf_format"
    assembly_check_report_dir = f"{validation_output_folder}/assembly_check"
    os.makedirs(vcf_validation_report_dir, exist_ok=True)
    os.makedirs(assembly_check_report_dir, exist_ok=True)

    validation_pipeline = NextFlowPipeline()
    for vcf_file_name in vcf_files:
        vcf_file_name_no_ext = _get_vcf_filename_without_extension(vcf_file_name)
        vcf_file_name_no_ext_and_path = os.path.basename(vcf_file_name_no_ext)
        vcf_validator_process = NextFlowProcess(process_name=f"vcf_validate_{vcf_file_name_no_ext_and_path}",
                                                command_to_run=f"{vcf_validator_binary} -i {vcf_file_name} "
                                                               f"-r database,text "
                                                               f"-o {vcf_validation_report_dir} "
                                                               f"--require-evidence "
                                                               f"> "
                                                               f"{vcf_file_name_no_ext_and_path}.vcf_validation.log")
        assembly_check_process = NextFlowProcess(process_name=f"asm_check_{vcf_file_name_no_ext_and_path}",
                                                 command_to_run=f"{assembly_checker_binary} -i {vcf_file_name} "
                                                                f"-f {fasta_path} -a {assembly_report_path}"
                                                                f"-r summary,text,valid "
                                                                f"-o {assembly_check_report_dir} "
                                                                f"--require-genbank "
                                                                f"> "
                                                                f"{vcf_file_name_no_ext_and_path}.asm_check.log")
        # All the validation processes can be run in parallel and hence don't need to be dependent on other processes
        validation_pipeline.add_process_dependency(process=vcf_validator_process, dependency=None)
        validation_pipeline.add_process_dependency(process=assembly_check_process, dependency=None)
    return validation_pipeline


def combine_all_stages(pipelines: List[NextFlowPipeline]) -> NextFlowPipeline:
    main_pipeline = pipelines[0]
    other_pipelines = pipelines[1:]
    for pipeline in other_pipelines:
        main_pipeline = NextFlowPipeline.join_pipelines(main_pipeline, pipeline)
    return main_pipeline


def update_app_config_with_project_dirs(app_config: dict, snapshot_name: str, project_dir: str) -> dict:
    app_config['vcf_dir'] = os.path.join(project_dir, project_dirs['valid'], snapshot_name)
    app_config['log_dir'] = os.path.join(project_dir, project_dirs['logs'], snapshot_name)
    return app_config


def get_snapshot_download_stage(download_url: str, download_target_dir: str):
    snapshot_download_stage = NextFlowPipeline()
    download_file_name = os.path.basename(download_url)
    # Use strip-components switch to avoid extracting with the directory structure 
    # since we have already created the requisite directory and passed it to download_target_dir
    snapshot_download_command = f'bash -c "cd {download_target_dir} && curl -O {download_file_name} && ' \
                                f'tar xzvf {download_file_name} --strip-components=1"'
    snapshot_download_stage.add_process_dependency(NextFlowProcess(process_name="download_covid19_snapshot_vcfs",
                                                                   command_to_run=snapshot_download_command),
                                                   dependency=None)
    return snapshot_download_stage


def ingest_covid19_submission(download_url: str, snapshot_name: str, project_dir: str, nextflow_config_file: str,
                              app_config_file: str, resume: bool):
    app_config = config_utils.get_args_from_private_config_file(app_config_file)
    snapshot_name = snapshot_name if snapshot_name else os.path.basename(download_url).replace(".tar.gz", "")
    assert snapshot_name, "Snapshot name cannot be empty!"
    app_config = update_app_config_with_project_dirs(app_config, snapshot_name, project_dir)

    download_files_stage = get_snapshot_download_stage(download_url, app_config['vcf_dir'])
    vcf_files = sorted(glob.glob(f"{app_config['vcf_dir']}/**/*.vcf.gz", recursive=True))
    vcf_preparation_stage = get_vcf_preparation_stage(vcf_files, app_config['bcftools_binary'])
    validation_stage = get_validation_stage(vcf_files,
                                            app_config['vcf_validator_binary'],
                                            app_config['assembly_checker_binary'],
                                            app_config['fasta_path'],
                                            app_config['assembly_report_path'],
                                            os.path.join(project_dir, "validations")
                                            )
    submission_pipeline = combine_all_stages([download_files_stage, vcf_preparation_stage, validation_stage])
    submission_pipeline.run_pipeline(workflow_file_path=os.path.join(app_config['submission_processing_dir'], "vertical_concat.nf"),
                                     nextflow_binary_path=app_config['nextflow_binary'], nextflow_config_path=nextflow_config_file,
                          resume=resume)


def main():
    parser = argparse.ArgumentParser(description='Vertically concatenate multiple VCF files in several stages',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--download-url",
                        help="URL to the data snapshot (ex: http://path/to/snapshots/YYYY_MM_DD.tar.gz)", required=True)
    parser.add_argument("--snapshot-name", help="Snapshot name (ex: 2021_06_28_filtered_vcf)", default=None,
                        required=False)
    parser.add_argument("--project-dir", help="Full path to the PRJEB directory", required=True)
    parser.add_argument("--app-config-file",
                        help="Full path to the application config file (ex: /path/to/config.yml)", required=True)
    parser.add_argument("--nextflow-config-file",
                        help="Full path to the Nextflow config file", default=None, required=False)
    parser.add_argument("--resume",
                        help="Indicate if a previous concatenation job is to be resumed", action='store_true',
                        required=False)
    args = parser.parse_args()
    ingest_covid19_submission(**vars(args))


if __name__ == "__main__":
    main()
