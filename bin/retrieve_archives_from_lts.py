from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.retrieve_eload_and_project_from_lts import ELOADRetrieval
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Retrieve ELOAD/Project from Long Term Storage')
    argparse.add_argument('--eload', required=False, type=int, help='The ELOAD to retrieve e.g. 919')
    argparse.add_argument('--retrieve_associated_project', action='store_true', default=False,
                          help='Retrieve the project associated with eload')
    argparse.add_argument('--update_path', action='store_true', default=False,
                          help='Update all noah paths to codon in ELOAD config file')
    argparse.add_argument('--eload_dirs_files', required=False, type=str, nargs='+', help='The Project to retrieve')
    argparse.add_argument('--project', required=False, type=str, help='The Project to retrieve')
    argparse.add_argument('--project_dirs_files', required=False, type=str, nargs='+', help='The Project to retrieve')
    argparse.add_argument('--eload_lts_dir', required=False, type=str,
                          help='The dir in lts where eloads are archived')
    argparse.add_argument('--project_lts_dir', required=False, type=str,
                          help='The dir in lts where project are archived')
    argparse.add_argument('--eload_retrieval_dir', required=False, type=str,
                          help='The output directory where archives will be retrieved')
    argparse.add_argument('--project_retrieval_dir', required=False, type=str,
                          help='The output directory where archives will be retrieved')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()

    if not args.eload and not args.project:
        raise ValueError(f'Need to provide either an Eload or a project to retrieve')
    if args.eload and not args.eload_lts_dir:
        raise ValueError(f'To retrieve Eload from lts, please provide path to the Eload archive dir in lts')
    if args.eload and args.retrieve_associated_project and not args.project_lts_dir:
        raise ValueError(
            f'If you want to retrieve the porject associated with eload, you need to provide project lts dir path')
    if args.project and not args.project_lts_dir:
        raise ValueError(f'To retrieve project from lts, please provide path to the project archive dir in lts')

    # Load the config_file from default location
    load_config()

    with ELOADRetrieval(args.eload) as eload_retrieval:
        eload_retrieval.retrieve_eloads_and_projects(args.eload, args.retrieve_associated_project, args.update_path,
                                                     args.eload_dirs_files, args.project, args.project_dirs_files,
                                                     args.eload_lts_dir, args.project_lts_dir, args.eload_retrieval_dir,
                                                     args.project_retrieval_dir)


if __name__ == "__main__":
    main()
