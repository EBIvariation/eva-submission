from eva_sub_cli_processing.sub_cli_submission import SubCliProcess
from eva_sub_cli_processing.sub_cli_utils import INGESTION


class SubCliProcessIngestion(SubCliProcess):

    processing_step = INGESTION


    def _start(self):
        pass

