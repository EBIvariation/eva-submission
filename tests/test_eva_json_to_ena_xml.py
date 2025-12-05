import json
import os
from datetime import datetime
from unittest import TestCase
from unittest.mock import patch
import xml.etree.ElementTree as ET

from eva_submission import ROOT_DIR
from eva_submission.ENA_submission.json_to_ENA_xml import EnaJson2XmlConverter
from tests.test_xlsx_to_xml import elements_equal


class TestEVAJsonToENAXmlConverter(TestCase):

    def setUp(self) -> None:
        self.brokering_folder = os.path.join(ROOT_DIR, 'tests', 'resources', 'brokering')
        self.metadata_file = os.path.join(self.brokering_folder, 'eva_metadata_json.json')
        self.converter = EnaJson2XmlConverter(
            'Submission-12345', self.metadata_file, self.brokering_folder,
            'To_Xml_Converter_Test'
        )

        self.project = {
            "title": "Example Project",
            "description": "An example project for demonstration purposes",
            "centre": "University of Example",
            "taxId": 9606,
            "parentProject": "PRJEB00001",
            "childProjects": ["PRJEB00002", "PRJEB00003"],
            "peerProjects": ["PRJEB00004", "PRJEB00005"],
            "publications": ["PubMed:123456", "PubMed:789012"],
            "links": ["http://www.abc.com|abc", "http://xyz.com", "PubMed:123456", "PubMed:789012:abcxyz"]
        }

        self.analysis = {
            'analysisTitle': 'Genomic Relationship Matrix',
            'analysisAlias': 'GRM',
            'description': 'A genomic relationship matrix (GRM) was computed',
            'Project Title': 'TechFish - Vibrio challenge',
            'experimentType': 'Genotyping by array',
            'referenceGenome': 'http://abc.com',
            'software': ['software package GCTA', 'Burrows-Wheeler Alignment tool (BWA)', 'HTSeq-python package'],
            'platform': "BGISEQ-500",
            'imputation': True,
            "links": ["http://www.abc.com|abc", "http://xyz.com", "PubMed:123456", "PubMed:789012:abcxyz"],

        }

        self.samples = [
            {'analysisAlias': 'GRM', 'sampleInVCF': '201903VIBRIO1185679118', 'bioSampleAccession': 'SAMEA7851610'},
            {'analysisAlias': 'GRM', 'sampleInVCF': '201903VIBRIO1185679119', 'bioSampleAccession': 'SAMEA7851611'},
            {'analysisAlias': 'GRM', 'sampleInVCF': '201903VIBRIO1185679120', 'bioSampleAccession': 'SAMEA7851612'},
            {'analysisAlias': 'GRM', 'sampleInVCF': '201903VIBRIO1185679121', 'bioSampleAccession': 'SAMEA7851613'},
            {'analysisAlias': 'GRM', 'sampleInVCF': '201903VIBRIO1185679122', 'bioSampleAccession': 'SAMEA7851614'},
        ]

        self.files = [
            {'analysisAlias': 'GRM', 'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz',
             'md5': 'c263a486e9b273d6e1e4c5f46ca5ccb8'},
            {'analysisAlias': 'GRM', 'fileName': 'Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi',
             'md5': '4b61e00524cc1f4c98e932b0ee27d94e'},
        ]

    @staticmethod
    def _delete_file(file_path):
        if os.path.exists(file_path):
            os.remove(file_path)

    def tearDown(self) -> None:
        self._delete_file(os.path.join(self.brokering_folder, 'To_Xml_Converter_Test.xml'))

    def test_create_ena_project_json_obj(self):
        expected_project = """<?xml version='1.0' encoding='utf8'?>
<PROJECT_SET>
	<PROJECT alias="Submission-12345" center_name="University of Example">
		<TITLE>Example Project</TITLE>
		<DESCRIPTION>An example project for demonstration purposes</DESCRIPTION>
		<PUBLICATIONS>
			<PUBLICATION>
				<PUBLICATION_LINKS>
					<PUBLICATION_LINK>
						<XREF_LINK>
							<DB>PubMed</DB>
							<ID>123456</ID>
						</XREF_LINK>
					</PUBLICATION_LINK>
				</PUBLICATION_LINKS>
			</PUBLICATION>
			<PUBLICATION>
				<PUBLICATION_LINKS>
					<PUBLICATION_LINK>
						<XREF_LINK>
							<DB>PubMed</DB>
							<ID>789012</ID>
						</XREF_LINK>
					</PUBLICATION_LINK>
				</PUBLICATION_LINKS>
			</PUBLICATION>
		</PUBLICATIONS>
		<SUBMISSION_PROJECT>
			<SEQUENCING_PROJECT />
			<ORGANISM>
				<TAXON_ID>9606</TAXON_ID>
				<SCIENTIFIC_NAME>Oncorhynchus mykiss</SCIENTIFIC_NAME>
			</ORGANISM>
		</SUBMISSION_PROJECT>
		<RELATED_PROJECTS>
			<RELATED_PROJECT>
				<PARENT_PROJECT accession="PRJEB00001" />
			</RELATED_PROJECT>
			<RELATED_PROJECT>
				<CHILD_PROJECT accession="PRJEB00002" />
			</RELATED_PROJECT>
			<RELATED_PROJECT>
				<CHILD_PROJECT accession="PRJEB00003" />
			</RELATED_PROJECT>
			<RELATED_PROJECT>
				<PEER_PROJECT accession="PRJEB00004" />
			</RELATED_PROJECT>
			<RELATED_PROJECT>
				<PEER_PROJECT accession="PRJEB00005" />
			</RELATED_PROJECT>
		</RELATED_PROJECTS>
		<PROJECT_LINKS>
			<PROJECT_LINK>
				<URL_LINK>
					<LABEL>abc</LABEL>
					<URL>http://www.abc.com</URL>
				</URL_LINK>
			</PROJECT_LINK>
			<PROJECT_LINK>
				<URL_LINK>
					<LABEL>http://xyz.com</LABEL>
					<URL>http://xyz.com</URL>
				</URL_LINK>
			</PROJECT_LINK>
			<PROJECT_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>123456</ID>
				</XREF_LINK>
			</PROJECT_LINK>
			<PROJECT_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>789012</ID>
					<LABEL>abcxyz</LABEL>
				</XREF_LINK>
			</PROJECT_LINK>
		</PROJECT_LINKS>
	</PROJECT>
</PROJECT_SET>"""

        with patch('eva_submission.ENA_submission.json_to_ENA_xml.get_scientific_name_from_ensembl') as m_sci_name:
            m_sci_name.return_value = 'Oncorhynchus mykiss'
            self.converter.eva_json_data['project'] = self.project
            root = self.converter._create_project_xml()
            expected_root = ET.fromstring(expected_project)
            assert elements_equal(root, expected_root)

    def test_add_analysis(self):
        expected_analysis = """<?xml version='1.0' encoding='utf8'?>
<ANALYSIS_SET>
	<ANALYSIS alias="GRM" center_name="University of Example">
		<TITLE>Genomic Relationship Matrix</TITLE>
		<DESCRIPTION>A genomic relationship matrix (GRM) was computed</DESCRIPTION>
		<STUDY_REF refname="Submission-12345" />
		<SAMPLE_REF accession="SAMEA7851610" label="201903VIBRIO1185679118" />
		<SAMPLE_REF accession="SAMEA7851611" label="201903VIBRIO1185679119" />
		<SAMPLE_REF accession="SAMEA7851612" label="201903VIBRIO1185679120" />
		<SAMPLE_REF accession="SAMEA7851613" label="201903VIBRIO1185679121" />
		<SAMPLE_REF accession="SAMEA7851614" label="201903VIBRIO1185679122" />
		<ANALYSIS_TYPE>
			<SEQUENCE_VARIATION>
				<ASSEMBLY>
					<CUSTOM>
						<URL_LINK>
							<URL>http://abc.com</URL>
						</URL_LINK>
					</CUSTOM>
				</ASSEMBLY>
				<EXPERIMENT_TYPE>Genotyping by array</EXPERIMENT_TYPE>
				<PROGRAM>software package GCTA</PROGRAM>
				<PROGRAM>Burrows-Wheeler Alignment tool (BWA)</PROGRAM>
				<PROGRAM>HTSeq-python package</PROGRAM>
				<PLATFORM>BGISEQ-500</PLATFORM>
				<IMPUTATION>1</IMPUTATION>
			</SEQUENCE_VARIATION>
		</ANALYSIS_TYPE>
		<FILES>
			<FILE filename="Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz" filetype="vcf" checksum_method="MD5" checksum="c263a486e9b273d6e1e4c5f46ca5ccb8" />
			<FILE filename="Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi" filetype="tabix" checksum_method="MD5" checksum="4b61e00524cc1f4c98e932b0ee27d94e" />
		</FILES>
		<ANALYSIS_LINKS>
			<ANALYSIS_LINK>
				<URL_LINK>
					<LABEL>abc</LABEL>
					<URL>http://www.abc.com</URL>
				</URL_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<URL_LINK>
					<LABEL>http://xyz.com</LABEL>
					<URL>http://xyz.com</URL>
				</URL_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>123456</ID>
				</XREF_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>789012</ID>
					<LABEL>abcxyz</LABEL>
				</XREF_LINK>
			</ANALYSIS_LINK>
		</ANALYSIS_LINKS>
		<ANALYSIS_ATTRIBUTES />
	</ANALYSIS>
</ANALYSIS_SET>"""
        root = ET.Element('ANALYSIS_SET')
        self.converter._add_analysis(root, self.analysis, self.samples, self.files, self.project)
        assert elements_equal(root, ET.fromstring(expected_analysis))

    def test_add_analysis_to_existing_project(self):
        # Override the cached property
        self.converter.existing_project = 'PRJEB00001'
        expected_analysis = """<?xml version='1.0' encoding='utf8'?>
<ANALYSIS_SET>
	<ANALYSIS alias="GRM" center_name="University of Example">
		<TITLE>Genomic Relationship Matrix</TITLE>
		<DESCRIPTION>A genomic relationship matrix (GRM) was computed</DESCRIPTION>
		<STUDY_REF accession="PRJEB00001" />
		<SAMPLE_REF accession="SAMEA7851610" label="201903VIBRIO1185679118" />
		<SAMPLE_REF accession="SAMEA7851611" label="201903VIBRIO1185679119" />
		<SAMPLE_REF accession="SAMEA7851612" label="201903VIBRIO1185679120" />
		<SAMPLE_REF accession="SAMEA7851613" label="201903VIBRIO1185679121" />
		<SAMPLE_REF accession="SAMEA7851614" label="201903VIBRIO1185679122" />
		<ANALYSIS_TYPE>
			<SEQUENCE_VARIATION>
				<ASSEMBLY>
					<CUSTOM>
						<URL_LINK>
							<URL>http://abc.com</URL>
						</URL_LINK>
					</CUSTOM>
				</ASSEMBLY>
				<EXPERIMENT_TYPE>Genotyping by array</EXPERIMENT_TYPE>
				<PROGRAM>software package GCTA</PROGRAM>
				<PROGRAM>Burrows-Wheeler Alignment tool (BWA)</PROGRAM>
				<PROGRAM>HTSeq-python package</PROGRAM>
				<PLATFORM>BGISEQ-500</PLATFORM>
				<IMPUTATION>1</IMPUTATION>
			</SEQUENCE_VARIATION>
		</ANALYSIS_TYPE>
		<FILES>
			<FILE filename="Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz" filetype="vcf" checksum_method="MD5" checksum="c263a486e9b273d6e1e4c5f46ca5ccb8" />
			<FILE filename="Vibrio.chrom.fix2.final.debug.gwassnps.vcf.gz.tbi" filetype="tabix" checksum_method="MD5" checksum="4b61e00524cc1f4c98e932b0ee27d94e" />
		</FILES>
		<ANALYSIS_LINKS>
			<ANALYSIS_LINK>
				<URL_LINK>
					<LABEL>abc</LABEL>
					<URL>http://www.abc.com</URL>
				</URL_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<URL_LINK>
					<LABEL>http://xyz.com</LABEL>
					<URL>http://xyz.com</URL>
				</URL_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>123456</ID>
				</XREF_LINK>
			</ANALYSIS_LINK>
			<ANALYSIS_LINK>
				<XREF_LINK>
					<DB>PubMed</DB>
					<ID>789012</ID>
					<LABEL>abcxyz</LABEL>
				</XREF_LINK>
			</ANALYSIS_LINK>
		</ANALYSIS_LINKS>
		<ANALYSIS_ATTRIBUTES />
	</ANALYSIS>
</ANALYSIS_SET>"""
        root = ET.Element('ANALYSIS_SET')
        self.converter._add_analysis(root, self.analysis, self.samples, self.files, self.project)
        assert elements_equal(root, ET.fromstring(expected_analysis))

    def test_create_submission_json_obj(self):
        expected_submission = """<SUBMISSION_SET>
	<SUBMISSION alias="Submission-12345" center_name="University of Example">
		<ACTIONS>
			<ACTION>
				<ADD />
			</ACTION>
			<ACTION>
				<HOLD HoldUntilDate="2025-01-04" />
			</ACTION>
		</ACTIONS>
	</SUBMISSION>
</SUBMISSION_SET>"""

        with patch('eva_submission.ENA_submission.json_to_ENA_xml.today',
                   return_value=datetime(year=2025, month=1, day=1)):
            root = self.converter._create_submission_single_xml('ADD', self.project)
            assert elements_equal(root, ET.fromstring(expected_submission))

    def _print_xml(self, root):
        ET.indent(root, space="\t", level=0)
        print(ET.tostring(root, encoding='utf8').decode("utf-8"))
