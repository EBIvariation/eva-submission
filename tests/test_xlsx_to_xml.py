import os
from unittest import TestCase
from xml.etree.ElementTree import ElementTree

from eva_submission.xlsx_to_xml.xlsx_to_ENA_xml import add_project, new_project, new_analysis, add_analysis, \
    process_metadata_spreadsheet, prettify


class TestXlsToXml(TestCase):
    project_row = {
        'Project Title': 'TechFish - Vibrio challenge',
        'Project Alias': 'TechFish',
        'Description': 'Identification of a major QTL for resistance to Vibrio anguillarum in rainbow trout',
        'Center': 'Laboratory of Aquatic Pathobiology, Department of Veterinary and Animal Sciences, Faculty of Health and Medical Sciences, University of Copenhagen',
        'Tax ID': 8022,
        'Publication(s)': 'PubMed:123456,PubMed:987654'
    }

    analysis_row = {
        'Analysis Title': 'Genomic Relationship Matrix',
        'Analysis Alias': 'GRM',
        'Description': 'A genomic relationship matrix (GRM) was computed, using all the high-quality SNPs on the SNP chip. This was used to account for polygenic effects (including family background effects). Fish from the same full-sib family necessarily share a large fraction of alleles, implying that their relationship will be high in the GRM (as would also be the case for a pedigree-based relationship matrix). In other words, a genomic animal model (GBLUP) is used to account for polygenic effects. In the GWAS, we utilized a leave-one-chromosome-out approach. This means that the GRM is set up using all SNPs, except those located on the same chromosome that is being currently tested (so that the GRM does not capture the effect of potential QTL on that chromosome, but general polygenic effects are captured). Using a linear model heritability is estimated on the observed scale. However, this estimate was also transformed to the underlying scale',
        'Project Title': 'TechFish - Vibrio challenge',
        'Experiment Type': 'Genotyping by array',
        'Reference': 'GCA_002163495.1',
        'Software': 'software package GCTA, Burrows-Wheeler Alignment tool (BWA), HTSeq-python package ',
    }

    sample_rows = [
        {'Analysis Alias': 'GRM', 'Sample ID': '201903VIBRIO1185679118', 'Sample Accession': 'SAMEA7851610'},
        {'Analysis Alias': 'GRM', 'Sample ID': '201903VIBRIO1185679119', 'Sample Accession': 'SAMEA7851611'},
        {'Analysis Alias': 'GRM', 'Sample ID': '201903VIBRIO1185679120', 'Sample Accession': 'SAMEA7851612'},
        {'Analysis Alias': 'GRM', 'Sample ID': '201903VIBRIO1185679121', 'Sample Accession': 'SAMEA7851613'},
        {'Analysis Alias': 'GRM', 'Sample ID': '201903VIBRIO1185679122', 'Sample Accession': 'SAMEA7851614'},
    ]

    file_rows = [
        {'Analysis Alias': 'GRM', 'File Name': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz',
         'File Type': 'vcf', 'MD5': 'c263a486e9b273d6e1e4c5f46ca5ccb8'},
        {'Analysis Alias': 'GRM', 'File Name': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi',
         'File Type': 'tabix', 'MD5': '4b61e00524cc1f4c98e932b0ee27d94e'},
    ]

    def test_add_project(self):
        root = new_project()
        add_project(root, self.project_row)
        print(prettify(ElementTree(root)))

    def test_add_analysis(self):
        root = new_analysis()
        add_analysis(root, self.analysis_row, self.project_row, self.sample_rows, self.file_rows)
        print(prettify(ElementTree(root)))

    def test_process_metadata_spreadsheet(self):
        brokering_folder = os.path.join(os.path.dirname(__file__), 'resources', 'brokering')
        metadata_file = os.path.join(brokering_folder, 'metadata_sheet.xlsx')
        process_metadata_spreadsheet(metadata_file, brokering_folder, 'TEST1')

