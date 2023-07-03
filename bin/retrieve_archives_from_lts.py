#!/usr/bin/env python

# Copyright 2020 EMBL - European Bioinformatics Institute
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

from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Retrieve ELOAD/Project from Long Term Storage')
    group = argparse.add_mutually_exclusive_group()
    group.add_argument('--eload', required=False, type=int, help='The ELOAD to retrieve e.g. 919')
    group.add_argument('--project', required=False, type=str, help='The Project to retrieve')
    argparse.add_argument('--retrieve_associated_project', action='store_true', default=False,
                          help='Retrieve the project associated with eload')
    argparse.add_argument('--update_path', action='store_true', default=False,
                          help='Update all noah paths to codon in ELOAD config file')
    argparse.add_argument('--eload_dirs_files', required=False, type=str, nargs='+',
                          help='Relative path to file or directories to retrieve in the eload archive')
    argparse.add_argument('--project_dirs_files', required=False, type=str, nargs='+',
                          help='Relative path to file or directories to retrieve in the project archive')
    argparse.add_argument('--eload_lts_dir', required=False, type=str,
                          help='The dir in lts where eloads are archived')
    argparse.add_argument('--project_lts_dir', required=False, type=str,
                          help='The dir in lts where project are archived')
    argparse.add_argument('--eload_retrieval_dir', required=False, type=str,
                          help='The output directory where archived ELOADs will be unpacked')
    argparse.add_argument('--project_retrieval_dir', required=False, type=str,
                          help='The output directory where archived project directories will be unpacked')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()

    if not args.eload and not args.project:
        raise ValueError(f'Need to provide either an Eload or a Project to retrieve')

    # Load the config_file from default location
    load_config()

    eload_retrieval = ELOADRetrieval()
    eload_retrieval.retrieve_eloads_and_projects(args.eload, args.retrieve_associated_project, args.update_path,
                                                 args.eload_dirs_files, args.project, args.project_dirs_files,
                                                 args.eload_lts_dir, args.project_lts_dir, args.eload_retrieval_dir,
                                                 args.project_retrieval_dir)


if __name__ == "__main__":
    main()
