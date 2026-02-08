#!/usr/bin/env python
import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from sqlalchemy import select

from eva_submission.submission_config import load_config
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import Project

logger = log_cfg.get_logger(__name__)


def update_file_sizes_for_project(loader, project_accession):
    """
    Update file_size in EVAPRO for all files associated with a project,
    using values retrieved from ENA. Only updates when MD5 checksums match.
    Returns a tuple of (updated, skipped, not_found) counts.
    """
    finder = loader.ena_project_finder
    file_updated = 0
    file_skipped = 0
    file_not_found = 0
    file_mismatch = 0
    submission_linked = 0
    submission_skipped = 0

    project_result = loader.eva_session.execute(
        select(Project).where(Project.project_accession == project_accession)
    ).fetchone()
    if not project_result:
        logger.error(f'Project {project_accession} not found in EVAPRO')
        return file_updated, file_skipped, file_not_found, file_mismatch, submission_linked, submission_skipped

    project = project_result.Project

    loader.begin_or_continue_transaction()
    for analysis_obj in project.analyses:
        for analysis_acc, submission_file_id, filename, file_md5, file_type, file_size, status_id \
                in finder.find_files_in_ena(analysis_obj.analysis_accession):
            file_obj = loader.get_file_for_analysis_and_name(analysis_obj.analysis_accession, filename)
            if file_obj is None:
                logger.warning(f'File {filename} from ENA analysis {analysis_acc} not found in EVAPRO')
                file_not_found += 1
                continue
            if file_obj.file_md5 != file_md5:
                logger.warning(
                    f'MD5 mismatch for {filename} in analysis {analysis_acc}: '
                    f'EVAPRO={file_obj.file_md5}, ENA={file_md5}. Skipping.'
                )
                file_mismatch += 1
                continue
            if file_obj.file_size != file_size:
                logger.info(f'Updating file_size for {filename}: {file_obj.file_size} -> {file_size}')
                file_obj.file_size = file_size
                file_updated += 1
            else:
                logger.debug(f'File {filename} already has correct size {file_size}')
                file_skipped += 1

        for submission_info in finder.find_ena_submission_for_analysis(analysis_accession=analysis_obj.analysis_accession):

            submission_id, alias, last_updated, hold_date, action = submission_info
            submission_obj = loader.insert_ena_submission(ena_submission_accession=submission_id,
                                                        action=action.get('type'),
                                                        submission_alias=alias, submission_date=last_updated,
                                                        brokered=1,
                                                        submission_type=action.get('schema', 'PROJECT').upper())
            if submission_obj not in analysis_obj.submissions:
                analysis_obj.submissions.append(submission_obj)
                submission_linked += 1
            else:
                logger.debug(f'Submission {submission_obj.submission_accession} already associated with analysis {analysis_obj.analysis_accession}')
                submission_skipped += 1

    loader.eva_session.commit()
    return file_updated, file_skipped, file_not_found, file_mismatch, submission_linked, submission_skipped


def main():
    argparse = ArgumentParser(
        description='Update file_size in EVAPRO for all files associated with a project, '
                    'using values from ENA.'
    )
    argparse.add_argument('--project_accession', required=True, type=str,
                          help='Project accession (e.g. PRJEB12345)')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set logging to debug level')
    args = argparse.parse_args()

    log_cfg.add_stdout_handler()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    load_config()

    loader = EvaProjectLoader()
    file_updated, file_skipped, file_not_found, file_mismatch, submission_linked, submission_skipped = update_file_sizes_for_project(loader, args.project_accession)
    logger.info(f'Done. File Updated: {file_updated}, File Skipped (already correct): {file_skipped}, '
                f'File Not found in EVAPRO: {file_not_found}, File Mismatching MD5 in EVAPRO: {file_mismatch}, '
                f'Submission Linked: {submission_linked}, Submission Skipped: {submission_skipped}')

    loader.eva_session.close()


if __name__ == "__main__":
    main()
