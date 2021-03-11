from unittest import TestCase

from eva_submission.eload_utils import compare_sample_sets


class TestEloadUtils(TestCase):

    def test_compare_sample_sets(self):
        assert compare_sample_sets([['S1', 'S2', 'S3'], ['S1', 'S2', 'S3']]) == 'single set'
        assert compare_sample_sets([['S1', 'S2', 'S3'], ['S1', 'S3', 'S2']]) == 'unsorted single set'
        assert compare_sample_sets([['S1'], ['S2'], ['S3']]) == 'unique sample sets'
        assert compare_sample_sets([['S1'], ['S2'], ['S3', 'S4']]) == 'unique sample sets'
        assert compare_sample_sets([['S1'], ['S2'], ['S3', 'S1']]) == 'overlapping sample names'
        assert compare_sample_sets([['S1'], ['S2'], ['S3', 'S3']]) == 'overlapping sample names'
