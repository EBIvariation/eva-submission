from eva_sub_cli_processing.sub_cli_submission import SubCliProcess
from eva_sub_cli_processing.sub_cli_utils import VALIDATION


class SubCliProcessValidation(SubCliProcess):

    processing_step = VALIDATION

    def _start(self):
        pass

