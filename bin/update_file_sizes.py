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

    project_result = loader.eva_session.execute(
        select(Project).where(Project.project_accession == project_accession)
    ).fetchone()
    if not project_result:
        logger.error(f'Project {project_accession} not found in EVAPRO')
        return 0, 0, 0

    project = project_result.Project
    updated = 0
    skipped = 0
    not_found = 0

    loader.begin_or_continue_transaction()
    for analysis in project.analyses:
        for analysis_acc, submission_file_id, filename, file_md5, file_type, file_size, status_id \
                in finder.find_files_in_ena(analysis.analysis_accession):
            file_obj = loader.get_file_for_analysis_and_name(analysis.analysis_accession, filename)
            if file_obj is None:
                logger.warning(f'File {filename} from ENA analysis {analysis_acc} not found in EVAPRO')
                not_found += 1
                continue
            if file_obj.file_md5 != file_md5:
                logger.warning(
                    f'MD5 mismatch for {filename} in analysis {analysis_acc}: '
                    f'EVAPRO={file_obj.file_md5}, ENA={file_md5}. Skipping.'
                )
                not_found += 1
                continue
            if file_obj.file_size != file_size:
                logger.info(f'Updating file_size for {filename}: {file_obj.file_size} -> {file_size}')
                file_obj.file_size = file_size
                updated += 1
            else:
                logger.debug(f'File {filename} already has correct size {file_size}')
                skipped += 1

    loader.eva_session.commit()
    logger.info(f'Done. Updated: {updated}, Skipped (already correct): {skipped}, Not found in EVAPRO: {not_found}')
    return updated, skipped, not_found


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
    update_file_sizes_for_project(loader, args.project_accession)
    loader.eva_session.close()


if __name__ == "__main__":
    main()
