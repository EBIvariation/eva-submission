import logging

import os
import argparse
import sys

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from eva_submission.submission_config import load_config
from eva_submission.samples_checker import compare_spreadsheet_and_vcf


def main():
    arg_parser = argparse.ArgumentParser(
        description='Transform and output validated data from an excel file to a XML file')
    arg_parser.add_argument('--metadata-file', required=True, dest='metadata_file',
                            help='EVA Submission Metadata Excel sheet')
    arg_parser.add_argument('--vcf-dir', required=True, dest='vcf_files_path',
                            help='Path to the directory in which submitted files can be found')
    arg_parser.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level', )
    args = arg_parser.parse_args()

    log_cfg.add_stdout_handler()

    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    compare_spreadsheet_and_vcf(args.metadata_file, args.vcf_dir)


if __name__ == "__main__":
    main()