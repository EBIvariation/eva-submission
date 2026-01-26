import os
import tempfile
from unittest import TestCase

import pysam

from eva_submission.steps.split_vcf_by_taxonomy import get_alt_allele_indices, load_sample_to_taxonomy_mapping, \
    split_vcf_by_sample,  split_vcf_by_taxonomy



class TestSplitVcfByTaxonomy(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        # Clean up temp files
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_get_alt_allele_indices(self):
        mock_sample = {'GT': (0, 1)}
        result = get_alt_allele_indices(mock_sample)
        assert result == {1}
        mock_sample = {'GT': (1, 1)}
        result = get_alt_allele_indices(mock_sample)
        assert result == {1}
        mock_sample = {'GT': (0, 0)}
        result = get_alt_allele_indices(mock_sample)
        assert result == set()
        mock_sample = {'GT': (None, None)}
        result = get_alt_allele_indices(mock_sample)
        assert result == set()
        mock_sample = {'GT': (1, 2)}
        result = get_alt_allele_indices(mock_sample)
        assert result == {1, 2}
        mock_sample = {'GT': (0, 2)}
        result = get_alt_allele_indices(mock_sample)
        assert result == {2}
        mock_sample = {'DP': 30}
        result = get_alt_allele_indices(mock_sample)
        assert result == set()

    def _create_test_vcf(self, content, filename='test.vcf'):
        """Create a test VCF file with given content."""
        vcf_path = os.path.join(self.temp_dir, filename)
        with open(vcf_path, 'w') as f:
            f.write(content)
        return vcf_path

    def _create_test_vcf_gz(self, content, filename='test.vcf.gz'):
        """Create a bgzipped test VCF file with given content (pysam requires bgzip)."""
        # First write uncompressed VCF
        uncompressed_path = self._create_test_vcf(filename.replace('.gz', ''))
        # Then read with pysam and write as bgzip
        vcf_path = os.path.join(self.temp_dir, filename)
        with pysam.VariantFile(uncompressed_path, 'r') as vcf_in:
            with pysam.VariantFile(vcf_path, 'w', header=vcf_in.header) as vcf_out:
                for record in vcf_in:
                    vcf_out.write(record)
        return vcf_path

    def _read_output_vcf(self, vcf_path):
        """Read an output VCF file and return records as list of dicts."""
        records = []
        with pysam.VariantFile(vcf_path, 'r') as vcf:
            samples = list(vcf.header.samples)
            for record in vcf:
                rec_dict = {
                    'chrom': record.chrom,
                    'pos': record.pos,
                    'ref': record.ref,
                    'alts': record.alts,
                    'samples': samples,
                    'genotypes': {}
                }
                for sample in samples:
                    rec_dict['genotypes'][sample] = record.samples[sample].get('GT')
                records.append(rec_dict)
        return records

    def test_basic_split_two_samples(self):
        """Test basic splitting of two-sample VCF."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
1	100	.	A	G	.	.	.	GT	0/1	0/0
1	200	.	C	T	.	.	.	GT	1/1	0/1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        # Check output files exist
        assert 'Sample1' in output_paths
        assert 'Sample2' in output_paths
        assert os.path.exists(output_paths['Sample1'])
        assert os.path.exists(output_paths['Sample2'])

        # Check Sample1 output
        sample1_records = self._read_output_vcf(output_paths['Sample1'])
        assert len(sample1_records) == 2  # Both variants have alt alleles
        assert sample1_records[0]['pos'] == 100
        assert sample1_records[0]['alts'] == ('G',)
        assert sample1_records[1]['pos'] == 200

        # Check Sample2 output - should only have variant at pos 200
        sample2_records = self._read_output_vcf(output_paths['Sample2'])
        assert len(sample2_records) == 1
        assert sample2_records[0]['pos'] == 200
        assert sample2_records[0]['genotypes']['Sample2'] == (0, 1)

    def test_exclude_hom_ref(self):
        """Test that homozygous reference variants are excluded."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	0/0
1	200	.	C	T	.	.	.	GT	0/1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        records = self._read_output_vcf(output_paths['Sample1'])
        assert len(records) == 1
        assert records[0]['pos'] == 200

    def test_exclude_missing_genotype(self):
        """Test that missing genotypes are excluded."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	./.
1	200	.	C	T	.	.	.	GT	1/1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        records = self._read_output_vcf(output_paths['Sample1'])
        assert len(records) == 1
        assert records[0]['pos'] == 200

    def test_multiallelic_split(self):
        """Test that multiallelic variants are split into biallelic records."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G,T	.	.	.	GT	1/2
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        records = self._read_output_vcf(output_paths['Sample1'])
        # Should have 2 records: one for G, one for T
        assert len(records) == 2

        # Check both alleles are present
        alts = {r['alts'][0] for r in records}
        assert alts == {'G', 'T'}

        # Each record should be biallelic
        for record in records:
            assert len(record['alts']) == 1

    def test_multiallelic_partial(self):
        """Test multiallelic where sample only has one of the alt alleles."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
1	100	.	A	G,T	.	.	.	GT	0/1	0/2
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        # Sample1 should only have G (allele 1)
        sample1_records = self._read_output_vcf(output_paths['Sample1'])
        assert len(sample1_records) == 1
        assert sample1_records[0]['alts'] == ('G',)

        # Sample2 should only have T (allele 2)
        sample2_records = self._read_output_vcf(output_paths['Sample2'])
        assert len(sample2_records) == 1
        assert sample2_records[0]['alts'] == ('T',)

    def test_genotype_remapping(self):
        """Test that genotypes are correctly remapped for biallelic records."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G,T	.	.	.	GT	1/2
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        records = self._read_output_vcf(output_paths['Sample1'])

        # For the G allele record: original 1/2 -> 1/0 (has the alt)
        # For the T allele record: original 1/2 -> 0/1 (has the alt)
        genotypes = {}
        for record in records:
            alt = record['alts'][0]
            genotypes[alt] = record['genotypes']['Sample1']

        # G was allele 1, so GT should be 1/0 or 0/1 (contains 1)
        assert 1 in genotypes['G']
        # T was allele 2, so GT should be 0/1 (remapped 2 -> 1)
        assert 1 in genotypes['T']

    def test_prefix_option(self):
        """Test that prefix is correctly applied to output filenames."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	0/1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir, prefix='test_')

        assert output_paths['Sample1'].endswith('test_Sample1.vcf.gz')
        assert os.path.exists(output_paths['Sample1'])

    def test_gzipped_input(self):
        """Test that gzipped VCF files are handled correctly."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	0/1
"""
        vcf_path = self._create_test_vcf_gz(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        records = self._read_output_vcf(output_paths['Sample1'])
        assert len(records) == 1
        assert records[0]['alts'] == ('G',)

    def test_phased_genotype(self):
        """Test that phased genotypes are preserved."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	0|1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        with pysam.VariantFile(output_paths['Sample1'], 'r') as vcf:
            for record in vcf:
                assert record.samples['Sample1'].phased is True

    def test_single_sample_header(self):
        """Test that output header contains only the single sample."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
1	100	.	A	G	.	.	.	GT	0/1	1/1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        # Check Sample1 header
        with pysam.VariantFile(output_paths['Sample1'], 'r') as vcf:
            assert list(vcf.header.samples) == ['Sample1']

        # Check Sample2 header
        with pysam.VariantFile(output_paths['Sample2'], 'r') as vcf:
            assert list(vcf.header.samples) == ['Sample2']

    def test_no_samples_returns_empty(self):
        """Test that VCF with no samples returns empty dict."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO
1	100	.	A	G	.	.	.
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        assert output_paths == {}

    def test_format_fields_preserved(self):
        """Test that FORMAT fields other than GT are preserved."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
##FORMAT=<ID=DP,Number=1,Type=Integer,Description="Read depth">
##FORMAT=<ID=GQ,Number=1,Type=Integer,Description="Genotype quality">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT:DP:GQ	0/1:30:99
"""
        vcf_path = self._create_test_vcf(vcf_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_sample(vcf_path, output_dir)

        with pysam.VariantFile(output_paths['Sample1'], 'r') as vcf:
            for record in vcf:
                assert record.samples['Sample1'].get('DP') == 30
                assert record.samples['Sample1'].get('GQ') == 99


class TestLoadSampleToTaxonomyMapping(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_tsv(self, content, filename='mapping.tsv'):
        """Create a TSV file with given content."""
        tsv_path = os.path.join(self.temp_dir, filename)
        with open(tsv_path, 'w') as f:
            f.write(content)
        return tsv_path

    def test_basic_mapping(self):
        """Test basic TSV parsing."""
        tsv_content = """Sample1	9606
Sample2	9606
Sample3	10090
"""
        tsv_path = self._create_tsv(tsv_content)

        sample_to_tax, tax_to_samples = load_sample_to_taxonomy_mapping(tsv_path)

        assert sample_to_tax == {'Sample1': '9606', 'Sample2': '9606', 'Sample3': '10090'}
        assert tax_to_samples == {'9606': ['Sample1', 'Sample2'], '10090': ['Sample3']}

    def test_whitespace_handling(self):
        """Test that whitespace is stripped from values."""
        tsv_content = """  Sample1  	  9606
Sample2	9606
"""
        tsv_path = self._create_tsv(tsv_content)

        sample_to_tax, tax_to_samples = load_sample_to_taxonomy_mapping(tsv_path)

        assert sample_to_tax == {'Sample1': '9606', 'Sample2': '9606'}


class TestSplitVcfByTaxonomy(TestCase):

    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        import shutil
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _create_test_vcf(self, content, filename='test.vcf'):
        """Create a test VCF file with given content."""
        vcf_path = os.path.join(self.temp_dir, filename)
        with open(vcf_path, 'w') as f:
            f.write(content)
        return vcf_path

    def _create_tsv(self, content, filename='mapping.tsv'):
        """Create a TSV file with given content."""
        tsv_path = os.path.join(self.temp_dir, filename)
        with open(tsv_path, 'w') as f:
            f.write(content)
        return tsv_path

    def _read_output_vcf(self, vcf_path):
        """Read an output VCF file and return records as list of dicts."""
        records = []
        with pysam.VariantFile(vcf_path, 'r') as vcf:
            samples = list(vcf.header.samples)
            for record in vcf:
                rec_dict = {
                    'chrom': record.chrom,
                    'pos': record.pos,
                    'ref': record.ref,
                    'alts': record.alts,
                    'samples': samples,
                    'genotypes': {}
                }
                for sample in samples:
                    rec_dict['genotypes'][sample] = record.samples[sample].get('GT')
                records.append(rec_dict)
        return records

    def test_basic_taxonomy_split(self):
        """Test basic splitting by taxonomy."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2	Sample3
1	100	.	A	G	.	.	.	GT	0/1	0/0	1/1
1	200	.	C	T	.	.	.	GT	0/0	0/1	0/0
"""
        tsv_content = """Sample1	human
Sample2	human
Sample3	mouse
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir)

        # Check output files exist for each taxonomy
        assert 'human' in output_paths
        assert 'mouse' in output_paths
        assert os.path.exists(output_paths['human'])
        assert os.path.exists(output_paths['mouse'])

        # Check human output - should have both samples
        human_records = self._read_output_vcf(output_paths['human'])
        assert len(human_records) == 2  # pos 100 (Sample1 has alt), pos 200 (Sample2 has alt)
        assert set(human_records[0]['samples']) == {'Sample1', 'Sample2'}

        # Check mouse output - should only have Sample3
        mouse_records = self._read_output_vcf(output_paths['mouse'])
        assert len(mouse_records) == 1  # Only pos 100 has alt for Sample3
        assert mouse_records[0]['samples'] == ['Sample3']
        assert mouse_records[0]['pos'] == 100

    def test_taxonomy_multiallelic_split(self):
        """Test multiallelic handling with taxonomy grouping."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2	Sample3
1	100	.	A	G,T	.	.	.	GT	0/1	0/2	0/0
"""
        tsv_content = """Sample1	taxA
Sample2	taxA
Sample3	taxB
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir)

        # taxA should have 2 records (G and T alleles, since Sample1 has G, Sample2 has T)
        taxA_records = self._read_output_vcf(output_paths['taxA'])
        assert len(taxA_records) == 2
        alts = {r['alts'][0] for r in taxA_records}
        assert alts == {'G', 'T'}

        # taxB should have no records (Sample3 is 0/0)
        taxB_records = self._read_output_vcf(output_paths['taxB'])
        assert len(taxB_records) == 0

    def test_taxonomy_shared_allele(self):
        """Test that multiple samples in taxonomy sharing an allele produce one record."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
1	100	.	A	G	.	.	.	GT	0/1	1/1
"""
        tsv_content = """Sample1	human
Sample2	human
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir)

        # human should have 1 record (both samples share the G allele)
        human_records = self._read_output_vcf(output_paths['human'])
        assert len(human_records) == 1
        assert human_records[0]['alts'] == ('G',)
        # Both samples should be present with their genotypes
        assert human_records[0]['genotypes']['Sample1'] == (0, 1)
        assert human_records[0]['genotypes']['Sample2'] == (1, 1)

    def test_missing_sample_in_mapping(self):
        """Test that samples without mapping are skipped with warning."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2	Sample3
1	100	.	A	G	.	.	.	GT	0/1	0/1	1/1
"""
        tsv_content = """Sample1	human
Sample2	human
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        # Sample3 is not in mapping, should be skipped
        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir)

        # Only human taxonomy should be present
        assert 'human' in output_paths
        assert len(output_paths) == 1

        # human file should only have Sample1 and Sample2
        human_records = self._read_output_vcf(output_paths['human'])
        assert human_records[0]['samples'] == ['Sample1', 'Sample2']

    def test_taxonomy_prefix(self):
        """Test prefix option for taxonomy output files."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1
1	100	.	A	G	.	.	.	GT	0/1
"""
        tsv_content = """Sample1	9606
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir, prefix='test_')

        assert output_paths['9606'].endswith('test_9606.vcf.gz')
        assert os.path.exists(output_paths['9606'])

    def test_genotype_remapping_multi_sample(self):
        """Test genotype remapping for multiallelic with multiple samples."""
        vcf_content = """##fileformat=VCFv4.2
##contig=<ID=1,length=1000000>
##FORMAT=<ID=GT,Number=1,Type=String,Description="Genotype">
#CHROM	POS	ID	REF	ALT	QUAL	FILTER	INFO	FORMAT	Sample1	Sample2
1	100	.	A	G,T	.	.	.	GT	1/2	0/1
"""
        tsv_content = """Sample1	tax1
Sample2	tax1
"""
        vcf_path = self._create_test_vcf(vcf_content)
        tsv_path = self._create_tsv(tsv_content)
        output_dir = os.path.join(self.temp_dir, 'output')

        output_paths = split_vcf_by_taxonomy(vcf_path, tsv_path, output_dir)

        records = self._read_output_vcf(output_paths['tax1'])
        assert len(records) == 2  # G and T alleles

        # Find the G record (allele 1 in original)
        g_record = next(r for r in records if r['alts'] == ('G',))
        # Sample1 has original GT=1/2, for G record: 1->1, 2->0 => (1,0)
        assert g_record['genotypes']['Sample1'] == (1, 0)
        # Sample2 has original GT=0/1, for G record: 0->0, 1->1 => (0,1)
        assert g_record['genotypes']['Sample2'] == (0, 1)

        # Find the T record (allele 2 in original)
        t_record = next(r for r in records if r['alts'] == ('T',))
        # Sample1 has original GT=1/2, for T record: 1->0, 2->1 => (0,1)
        assert t_record['genotypes']['Sample1'] == (0, 1)
        # Sample2 has original GT=0/1, for T record: 0->0, 1->0 => (0,0)
        assert t_record['genotypes']['Sample2'] == (0, 0)
