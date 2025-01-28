import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_sub_cli_processing.sub_cli_to_eload_converter.sub_cli_to_eload_converter import SubCLIToEloadConverter
from eva_submission.submission_config import load_config


def main():
    argparse = ArgumentParser(description='Convert a sub cli submission to an eload based submission')
    argparse.add_argument('--submission_account_id', required=True, type=str,
                          help='submission account id of the user which is generally "userId_loginType"')
    argparse.add_argument('--submission_id', required=True, type=str, help='The Submission Id of the submission')
    argparse.add_argument('--eload', required=True, type=int, help='The ELOAD number for the submission')
    argparse.add_argument('--taxid', required=False, type=str,
                          help='Override and replace the taxonomy id provided in the metadata')
    argparse.add_argument('--reference', required=False, type=str,
                          help='Override and replace the reference sequence accession provided in the metadata')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    with SubCLIToEloadConverter(args.eload) as sub_cli_to_eload:
        sub_cli_to_eload.retrieve_vcf_files_from_sub_cli_ftp_dir(args.submission_account_id, args.submission_id)
        sub_cli_to_eload.download_metadata_json_and_convert_to_xlsx(args.submission_id)
        sub_cli_to_eload.detect_all(args.taxid, args.reference)


if __name__ == "__main__":
    main()
