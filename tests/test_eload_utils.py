from unittest import TestCase

from eva_submission.eload_utils import check_existing_project


class TestEloadUtils(TestCase):

    def test_check_existing_project(self):
        assert check_existing_project('PRJ') is False
        assert check_existing_project('PRJEB42148') is True
