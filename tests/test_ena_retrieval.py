from unittest import TestCase

from eva_submission.ena_retrieval import files_from_ena_project


class TestEnaRetreival(TestCase):

    def test_files_from_ena_project(self):
        # files_from_ena_project('PRJEB21794')
        files_from_ena_project('ERZ468492')
        # files_from_ena_project('PRJEB28874')
