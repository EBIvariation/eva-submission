from unittest import TestCase

from eva_submission.eload_utils import retrieve_assembly_accession_from_ncbi


class TestValidation(TestCase):

    def test_retrieve_assembly_accession(self):
        # retrieve_assembly_accession('hg38')
        retrieve_assembly_accession_from_ncbi('GCA_000001405.15')

