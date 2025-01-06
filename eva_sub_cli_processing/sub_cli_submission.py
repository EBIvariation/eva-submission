import os
import random
import string
from datetime import datetime

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle

from eva_sub_cli_processing.sub_cli_utils import sub_ws_url_build, get_from_sub_ws, FAILURE, put_to_sub_ws, SUCCESS, \
    RUNNING

submission_logging_files = set()


class SubCliProcess(AppLogger):
    processing_step = None  # Should be set in subclasses

    def __init__(self, submission_id: str):
        self.submission_id = submission_id
        self.submission_dir = os.path.abspath(os.path.join(cfg['submission_dir'], self.submission_id))
        os.makedirs(self.submission_dir, exist_ok=True)
        self.create_log_file()

    @cached_property
    def submission_detail(self):
        return get_from_sub_ws(sub_ws_url_build('admin', 'submission', self.submission_id))

    def create_nextflow_temp_output_directory(self, base=None):
        random_string = ''.join(random.choice(string.ascii_letters) for i in range(6))
        if base is None:
            output_dir = os.path.join(self.submission_dir, 'nextflow_output_' + random_string)
        else:
            output_dir = os.path.join(base, 'nextflow_output_' + random_string)
        os.makedirs(output_dir)
        return output_dir

    @cached_property
    def now(self):
        return datetime.now()

    def create_log_file(self):
        logfile_name = os.path.join(self.submission_dir, "submission.log")
        if logfile_name not in submission_logging_files:
            log_cfg.add_file_handler(logfile_name)
            submission_logging_files.add(logfile_name)

    def start(self):
        """Start the processing while monitoring for exception"""
        try:
            put_to_sub_ws(sub_ws_url_build('admin', 'submission-process', self.submission_id, self.processing_step,
                                           RUNNING))
            self._start()
        except Exception as e:
            put_to_sub_ws(sub_ws_url_build('admin', 'submission-process', self.submission_id, self.processing_step,
                                           FAILURE))
            raise e
        put_to_sub_ws(sub_ws_url_build('admin', 'submission-process', self.submission_id, self.processing_step,
                                       SUCCESS))

    def _start(self):
        raise NotImplementedError

