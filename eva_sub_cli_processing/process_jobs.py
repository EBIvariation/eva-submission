from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.logger import AppLogger

from eva_sub_cli_processing.sub_cli_brokering import SubCliProcessBrokering
from eva_sub_cli_processing.sub_cli_ingestion import SubCliProcessIngestion
from eva_sub_cli_processing.sub_cli_utils import get_from_sub_ws, sub_ws_url_build, VALIDATION, READY_FOR_PROCESSING, \
    PROCESSING, BROKERING, INGESTION, SUCCESS, FAILURE, put_to_sub_ws
from eva_sub_cli_processing.sub_cli_validation import SubCliProcessValidation


def process_submissions():

    scanner = NewSubmissionScanner()
    ready_submissions = scanner.scan()

    if not ready_submissions:
        return 0
    else:
        # Process a defined set of new dataset found. Run through Cron, this will result in one new pipeline
        # being kicked off per interval.
        for ready_submission in ready_submissions:
            return _process_submission(ready_submission)


def _process_submission(submission):
    # TODO: Create submission processing directory
    submission.start()


class SubmissionScanner(AppLogger):

    statuses = []
    step_statuses = []

    def _scan_per_status(self):
        submissions = []
        for status in self.statuses:
            for submission_data in get_from_sub_ws(sub_ws_url_build('admin', 'submissions', 'status', status)):
                submissions.append(SubmissionStep(
                    submission_id=submission_data.get('submissionId'),
                    status=submission_data.get('status'),
                    processing_step=VALIDATION,
                    processing_status=READY_FOR_PROCESSING,
                    last_update_time=submission_data.get('uploadedTime'),
                    priority=5
                ))
        return submissions

    def _scan_per_step_status(self):
        submissions = []
        for step, status in self.step_statuses:
            for submission_step_data in get_from_sub_ws(sub_ws_url_build('admin', 'submission-processes', step, status)):
                submissions.append(SubmissionStep(
                    submission_id=submission_step_data.get('submissionId'),
                    status=PROCESSING,
                    processing_step=step,
                    processing_status=status,
                    last_update_time=submission_step_data.get('lastUpdateTime'),
                    priority=submission_step_data.get('priority')
                ))
        return submissions

    def scan(self):
        return self._scan_per_status() + self._scan_per_step_status()

    def report(self):
        header = ['Submission Id', 'Submission status', 'Processing step', 'Processing status', 'Last updated time',
                  'Priority']
        scan = self.scan()
        lines = []
        for submission in scan:
            lines.append((submission.submission_id, submission.submission_status, submission.processing_step,
                          submission.processing_status, str(submission.last_update_time), str(submission.priority)))
        pretty_print(header, lines)


class NewSubmissionScanner(SubmissionScanner):

    statuses = ['UPLOADED']
    step_statuses = []


class SubmissionStep(AppLogger):

    def __init__(self, submission_id, status, processing_step, processing_status, last_update_time, priority):
        self.submission_id = submission_id
        self.submission_status = status
        self.processing_step = processing_step
        self.processing_status = processing_status
        self.last_update_time = last_update_time
        self.priority = priority

    def start(self):
        self._set_next_step()
        self._update_submission_ws()
        self.submit_pipeline()

    def submit_pipeline(self):
        assert self.processing_status == READY_FOR_PROCESSING
        # TODO: These jobs needs to be submitted as independent processes
        if self.processing_step == VALIDATION:
            process = SubCliProcessValidation(self.submission_id)
        elif self.processing_step == BROKERING:
            process = SubCliProcessBrokering(self.submission_id)
        elif self.processing_step == INGESTION:
            process = SubCliProcessIngestion(self.submission_id)
        process.start()

    def _set_next_step(self):
        if self.submission_status != PROCESSING and not self.processing_step:
            self.submission_status = PROCESSING
            self.processing_step = VALIDATION
            self.processing_status = READY_FOR_PROCESSING
        elif self.processing_status == SUCCESS and self.processing_step == VALIDATION:
            self.processing_step = BROKERING
            self.processing_status = READY_FOR_PROCESSING
        elif self.processing_status == SUCCESS and self.processing_step == BROKERING:
            self.processing_step = INGESTION
            self.processing_status = READY_FOR_PROCESSING
        elif self.processing_status == FAILURE:
            # TODO: Is there something we need to do before restarting a failed job
            self.processing_status = READY_FOR_PROCESSING

    def _update_submission_ws(self):
        put_to_sub_ws(sub_ws_url_build('admin', 'submission', self.submission_id, 'status', self.submission_status))
        put_to_sub_ws('admin', 'submission-process', self.submission_id, self.processing_step, self.processing_status)

    def __repr__(self):
        return f'Submission(submission_id={self.submission_id}, submission_status={self.submission_status}, ' \
               f'processing_step={self.processing_step}, processing_status={self.processing_status}' \
               f'last_update_time={self.last_update_time})'



