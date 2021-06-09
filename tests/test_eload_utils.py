import os
import unittest

from eva_submission import ROOT_DIR
from eva_submission.eload_utils import backup_file


def touch(f):
    open(f, 'w').close()


class TestEloadUtils(unittest.TestCase):
    resources_folder = os.path.join(ROOT_DIR, 'tests', 'resources')

    def tearDown(self) -> None:
        test = os.path.join(self.resources_folder, 'testbackupfile.txt')
        for i in range(5):
            os.remove(test + '.' + str(i))

    def test_backup_file(self):
        test = os.path.join(self.resources_folder, 'testbackupfile.txt')
        touch(test)
        for i in range(4):
            touch(test + '.' + str(i))
        backup_file(test)
        assert not os.path.exists(test)
        for i in range(5):
            assert os.path.exists(test + '.' + str(i))
