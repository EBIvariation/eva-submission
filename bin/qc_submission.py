from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission.submission_qc_checks import EloadQC
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Run QC checks on the submitted eload')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number of the submission.')

    args = argparse.parse_args()

    log_cfg.add_stdout_handler()

    # Load the config_file from default location
    load_config()

    with EloadQC(args.eload) as eload_qc:
        eload_qc.run_qc_checks_for_submission()


if __name__ == "__main__":
    main()
