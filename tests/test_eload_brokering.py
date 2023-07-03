import csv
import glob
import os
import shutil
from unittest import TestCase
from unittest.mock import patch, PropertyMock

import yaml
from ebi_eva_common_pyutils.config import cfg

from eva_submission import NEXTFLOW_DIR
from eva_submission.biosamples_submission import SampleMetadataSubmitter
from eva_submission.eload_brokering import EloadBrokering
from eva_submission.eload_submission import Eload
from eva_submission.submission_config import load_config
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader
from tests.test_eload_preparation import touch


class TestEloadBrokering(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self) -> None:
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)
        self.eload = EloadBrokering(3)
        # Ensure we've cleared any past brokering status
        self.eload.eload_cfg.pop('brokering', 'Biosamples')
        self.existing_eload = EloadBrokering(4)

    def tearDown(self) -> None:
        eloads = glob.glob(os.path.join(self.resources_folder, 'eloads', 'ELOAD_3'))
        for eload in eloads:
            shutil.rmtree(eload)

    def test_upload_to_bioSamples(self):
        self.eload.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                                 value=os.path.join(self.resources_folder, 'metadata.xlsx'))

        samples = {f'S{i}': i for i in range(1, 101)}
        with patch.object(SampleMetadataSubmitter, 'submit_to_bioSamples', return_value=samples) as mock_submit,\
                patch.object(EloadBrokering, 'now', new_callable=PropertyMock(return_value='a_date')):
            self.eload.upload_to_bioSamples()

        assert mock_submit.call_count == 1
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'pass')
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'Samples') == samples
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'date') == 'a_date'

    def test_upload_to_bioSamples_incomplete(self):
        self.eload.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                                 value=os.path.join(self.resources_folder, 'metadata.xlsx'))

        incomplete = {'S1': 1}
        with patch.object(SampleMetadataSubmitter, 'submit_to_bioSamples', return_value=incomplete) as mock_submit,\
                patch.object(EloadBrokering, 'now', new_callable=PropertyMock(return_value='a_date')):
            with self.assertRaises(ValueError):
                self.eload.upload_to_bioSamples()

        assert mock_submit.call_count == 1
        assert not self.eload.eload_cfg.query('brokering', 'Biosamples', 'pass')
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'Samples') == incomplete
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'date') == 'a_date'

    def test_upload_to_bioSamples_not_required(self):
        self.eload.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                                 value=os.path.join(self.resources_folder, 'metadata.xlsx'))

        with patch.object(SampleMetadataSubmitter, 'check_submit_done', return_value=True) as check_submit_done:
            self.eload.upload_to_bioSamples()
        assert check_submit_done.call_count == 2
        assert check_submit_done.call_count == 2
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'pass')

    def test_upload_to_bioSamples_done(self):
        self.eload.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet',
                                 value=os.path.join(self.resources_folder, 'metadata.xlsx'))
        self.eload.eload_cfg.set('brokering', 'Biosamples', 'Samples', value={'sample1': 'SAMPLE1'})
        self.eload.eload_cfg.set('brokering', 'Biosamples', 'pass', value=True)
        self.eload.upload_to_bioSamples()

    def test_run_brokering_prep_workflow(self):
        self.eload.eload_cfg.set('validation', 'valid', 'analyses', value={
            'analysis_alias1': {
                'vcf_files': [os.path.join(self.resources_folder, 'vcf_file.vcf')],
            },
        })
        self.eload.eload_cfg.set('submission', 'analyses', value={
            'analysis_alias1': {
                'assembly_fasta': os.path.join(self.resources_folder, 'reference_genome.fa'),
                'assembly_report': os.path.join(self.resources_folder, 'reference_genome_assembly_report.tx'),
                'assembly_accession': 'GCA000000.1'
            },
        })
        cfg.content['executable'] = {
            'nextflow': 'path_to_nextflow',
            'python': {'script_path': 'path_to_script'}
        }
        temp_dir = 'temporary_directory'
        nf_script = os.path.join(NEXTFLOW_DIR, 'prepare_brokering.nf')
        config_file = os.path.join(self.eload.eload_dir, 'brokering_config_file.yaml')

        with patch('eva_submission.eload_brokering.command_utils.run_command_with_output') as m_execute, \
                patch.object(Eload, 'create_nextflow_temp_output_directory', return_value=temp_dir):
            self.eload._run_brokering_prep_workflow()
        m_execute.assert_called_once_with(
            'Nextflow brokering preparation process',
            f'path_to_nextflow {nf_script} -params-file {config_file} -work-dir {temp_dir}'
        )

    def test_parse_bcftools_norm_report(self):
        normalisation_log = os.path.join(self.resources_folder, 'validations', 'bcftools_norm.log')
        expected = ([], 2, 0, 1, 0)
        assert self.eload.parse_bcftools_norm_report(normalisation_log) == expected

    def test_parse_bcftools_norm_report_failed(self):
        normalisation_log = os.path.join(self.resources_folder, 'validations', 'failed_bcftools_norm.log')
        expected = (["Reference allele mismatch at 20:17331 .. REF_SEQ:'TA' vs VCF:'GT'"], 0, 0, 0, 0)
        assert self.eload.parse_bcftools_norm_report(normalisation_log) == expected


    def test_collect_brokering_workflow_results(self):
        output_dir = os.path.join(self.eload.eload_dir, 'output')
        os.makedirs(output_dir)
        for f in ['vcf_file1.vcf.gz']:
            touch(os.path.join(output_dir, f))
        for f in ['vcf_file1.vcf.gz.md5', 'vcf_file1.vcf.gz.csi', 'vcf_file1.vcf.gz.csi.md5']:
            touch(os.path.join(output_dir, f), content=f'md5checksum {f}')
        touch(os.path.join(output_dir, 'vcf_file1.vcf_bcftools_norm.log'),
              content=f'Lines   total/split/realigned/skipped:  2/0/1/0')
        self.eload.eload_cfg.set('validation', 'valid', 'analyses', 'analysis alias 1', value={
            'assembly_accession': 'GCA_000001000.1',
            'assembly_fasta': 'fasta.fa',
            'assembly_report': 'assembly_report.txt',
            'vcf_files': ['vcf_file1.vcf']
        })
        self.eload._collect_brokering_prep_results(output_dir)
        vcf_file1 = os.path.join(self.eload.eload_dir, '18_brokering/ena/vcf_file1.vcf.gz')
        vcf_file1_csi = os.path.join(self.eload.eload_dir, '18_brokering/ena/vcf_file1.vcf.csi')
        assert os.path.isfile(vcf_file1)
        assert os.path.isfile(vcf_file1_csi)
        assert self.eload.eload_cfg['brokering']['analyses'] == {
            'analysis alias 1': {
                'assembly_accession': 'GCA_000001000.1',
                'assembly_fasta': 'fasta.fa',
                'assembly_report': 'assembly_report.txt',
                'vcf_files': {
                    vcf_file1: {
                        'original_vcf': 'vcf_file1.vcf',
                        'output_vcf_file': vcf_file1,
                        'md5': 'md5checksum',
                        'csi': vcf_file1_csi,
                        'csi_md5': 'md5checksum',
                        'normalisation': {'error_list': [], 'nb_realigned': 1, 'nb_skipped': 0, 'nb_split': 0,
                                          'nb_variant': 2}
                    }
                }
            }
        }

    def test_report(self):
        expected_report = '''Brokering performed on 2021-01-01 12:20:.0
BioSamples: PASS
ENA: PASS
----------------------------------

BioSamples brokering:
  * Biosamples: PASS
    - Accessions: S1: SAMEA0000001
S2: SAMEA0000002

----------------------------------

ENA brokering:
  * ENA: PASS
    - Hold date: 
    - Accessions: PROJECT: PRJEB00001
SUBMISSION: ERA0000001
ANALYSIS: ERZ0000001
    - Errors: 
    - receipt: <?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="receipt.xsl"?>
<RECEIPT receiptDate="2021-01-01T12:01:001.0Z" submissionFile="ELOAD_3.Submission.xml" success="true">
     <ANALYSIS accession="ERZ0000001" alias="alias1" status="PRIVATE"/>
     <PROJECT accession="PRJEB00001" alias="alias1" status="PRIVATE" holdUntilDate="2023-02-16Z">
          <EXT_ID accession="ERP000001" type="study"/>
     </PROJECT>
     <SUBMISSION accession="ERA0000001" alias="alias1"/>
     <MESSAGES>
          <INFO>Submission has been committed.</INFO>
     </MESSAGES>
     <ACTIONS>ADD</ACTIONS>
     <ACTIONS>ADD</ACTIONS>
</RECEIPT>

----------------------------------

Archival Confirmation Text:

Your EVA submission "Greatest project ever" has now been archived and will be made available to the public on 2021-01-04. The accessions associated with your submission are:
Project: PRJEB00001
Analyses: ERZ0000001 
If you wish your data to be held private beyond the date specified above, please let us know. Once released, the data will be made available to download from this link: https://www.ebi.ac.uk/eva/?eva-study=PRJEB00001
Please allow at least 48 hours from the initial release date provided for the data to be made available through this link. Each variant will be issued a unique SS# ID which will be made available to download via the "browsable files" link on the EVA study page.
You can also notify us when your paper has been assigned a PMID. We will add this to your study page in the EVA. If there is anything else you need please do not hesitate to notify me. Archived data can be referenced using the project accession & associated URL e.g. The variant data for this study have been deposited in the European Variation Archive (EVA) at EMBL-EBI under accession number PRJEB00001 (https://www.ebi.ac.uk/eva/?eva-study=PRJEB00001)
The EVA can be cited directly using the associated literature:
Cezard T, Cunningham F, Hunt SE, Koylass B, Kumar N, Saunders G, Shen A, Silva AF, Tsukanov K, Venkataraman S, Flicek P, Parkinson H, Keane TM. The European Variation Archive: a FAIR resource of genomic variation for all species. Nucleic Acids Res. 2021 Oct 28:gkab960. doi: 10.1093/nar/gkab960. PMID: 34718739.
        
'''
        with patch('builtins.print') as mprint:
            self.existing_eload.eload_cfg.set('submission', 'metadata_spreadsheet', value=os.path.join(self.existing_eload.eload_dir, '10_submitted/metadata_file/metadata_sheet.xlsx'))
            self.existing_eload.report()
        mprint.assert_called_once_with(expected_report)

    def test_update_metadata_from_config_for_files(self):
        # This metadata file contains multiple analysis each containing multiple files.
        # the file get merged into 1 file per analysis
        metadata_file = os.path.join(self.resources_folder, 'metadata_2_analysis.xlsx')
        ena_metadata_file = os.path.join(self.eload.eload_dir, 'metadata_2_analysis_for_brokering.xlsx')
        analyses = {
            'GAE': {
                'assembly_accession': 'GCA_000001405.1',
                'vcf_files': {
                    'path/to/GAE.vcf.gz': {
                      'csi': 'path/to/GAE.vcf.gz.csi',
                      'csi_md5': '',
                      'md5': '',
                      'original_vcf': 'path/to/original_GAE.vcf.gz',
                      'output_vcf_file': None
                    }
                }
            },
            'GAE2': {
                'assembly_accession': 'GCA_000001405.1',
                'vcf_files': {
                    'path/to/GAE2.vcf.gz': {
                        'csi': 'path/to/GAE2.vcf.gz.csi',
                        'csi_md5': '',
                        'md5': '',
                        'original_vcf': 'path/to/original_GAE2.vcf.gz',
                        'output_vcf_file': None
                    }
                }
            }
        }
        self.eload.eload_cfg.set('brokering', 'analyses', value=analyses)
        self.eload.update_metadata_spreadsheet(metadata_file, ena_metadata_file)

        # Check that the Files get set to the merged file name and that the analysis alias is modified
        reader = EvaXlsxReader(ena_metadata_file)
        assert reader.files == [
            {'Analysis Alias': 'GAE', 'File Name': 'ELOAD_3/GAE.vcf.gz', 'File Type': 'vcf', 'MD5': None, 'row_num': 2},
            {'Analysis Alias': 'GAE', 'File Name': 'ELOAD_3/GAE.vcf.gz.csi', 'File Type': 'csi', 'MD5': None, 'row_num': 3},
            {'Analysis Alias': 'GAE2', 'File Name': 'ELOAD_3/GAE2.vcf.gz', 'File Type': 'vcf', 'MD5': None, 'row_num': 4},
            {'Analysis Alias': 'GAE2', 'File Name': 'ELOAD_3/GAE2.vcf.gz.csi', 'File Type': 'csi', 'MD5': None, 'row_num': 5}
        ]
