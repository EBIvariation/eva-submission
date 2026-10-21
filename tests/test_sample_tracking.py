import datetime
import glob
import json
import os
import shutil
from copy import deepcopy
from unittest import TestCase
from unittest.mock import patch, PropertyMock, Mock

from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from sqlalchemy import create_engine, select

from eva_submission.ENA_submission.json_to_ENA_json import EnaJsonConverter
from eva_submission.biosample_submission.biosamples_submitters import BioSamplesSubmitter
from eva_submission.eload_brokering import EloadBrokering
from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.evapro.table import metadata, SampleInFile
from eva_submission.submission_config import load_config, EloadConfig


class TestAlleleFrequencySampleTracking(TestCase):
    """
    Brokering -> update_metadata_json -> ENA JSON -> EVAPRO load for allele frequency analyses,
    where sampleInVCF is optional.
    """
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    existing_samples = {
        'SAME000001': {'accession': 'SAME000001', 'name': 'bsd_name1', 'characteristics': {}},
        'SAME000002': {'accession': 'SAME000002', 'name': 'bsd_name2', 'characteristics': {}},
    }

    analysis = {
        'analysisAlias': 'AF1',
        'analysisTitle': 'Allele frequencies',
        'description': 'Allele frequencies of a population',
        'referenceGenome': 'GCA_000001405.1',
        'experimentType': 'Whole genome sequencing',
    }

    def setUp(self) -> None:
        load_config(os.path.join(self.resources_folder, 'submission_config.yml'))
        os.chdir(self.top_dir)
        self.eload = EloadBrokering(3)
        self.eload.eload_cfg.pop('brokering', 'Biosamples')
        self.loader = EvaProjectLoader()

    def tearDown(self) -> None:
        EloadConfig.content = {}
        for eload_dir in glob.glob(os.path.join(self.resources_folder, '*eloads', 'ELOAD_3')):
            shutil.rmtree(eload_dir)

    @staticmethod
    def _get_fake_sample(accession, include_curation=False):
        return deepcopy(TestAlleleFrequencySampleTracking.existing_samples.get(accession))

    @staticmethod
    def _fake_follows_link(*args, method='GET', join_url=None, json=None, **kwargs):
        if method == 'POST' and join_url is None:
            return {**json, 'accession': 'SAMEA_NEW_' + json.get('name', '')}
        return {}

    @staticmethod
    def _new_sample(name, sample_in_vcf=None):
        sample = {'analysisAlias': ['AF1'],
                  'bioSampleObject': {'name': name, 'characteristics': {'scientific name': [{'text': 'Homo sapiens'}]}}}
        if sample_in_vcf:
            sample['sampleInVCF'] = sample_in_vcf
        return sample

    def _broker(self, samples):
        metadata_json_file = os.path.join(self.eload.eload_dir, 'metadata.json')
        with open(metadata_json_file, 'w') as open_file:
            json.dump({'project': {'title': 'Project'}, 'analysis': [self.analysis], 'sample': samples,
                       'files': []}, open_file)
        self.eload.eload_cfg.set('validation', 'valid', 'metadata_json', value=metadata_json_file)
        self.eload.eload_cfg.set('brokering', 'analyses', value={
            'ELOAD_3_AF1': {'assembly_accession': 'GCA_000001405.1', 'vcf_files': {}}
        })
        with patch.object(BioSamplesSubmitter, '_get_existing_sample', side_effect=self._get_fake_sample), \
                patch.object(WebinHALCommunicator, 'follows_link', side_effect=self._fake_follows_link), \
                patch.object(WebinHALCommunicator, 'communicator_attributes', new_callable=PropertyMock,
                             return_value={'webinSubmissionAccountId': 'Webin-1'}):
            self.eload.upload_to_bioSamples()
        assert self.eload.eload_cfg.query('brokering', 'Biosamples', 'pass')

        ena_metadata_json_file = os.path.join(self.eload.eload_dir, 'metadata_for_ena.json')
        self.eload.update_metadata_json(metadata_json_file, ena_metadata_json_file)
        with open(ena_metadata_json_file) as open_file:
            ena_metadata = json.load(open_file)
        converter = EnaJsonConverter('ELOAD_3', ena_metadata_json_file, self.eload.eload_dir, 'ELOAD_3')
        return converter._add_analysis(ena_metadata['analysis'][0], ena_metadata['sample'], [],
                                       ena_metadata['project'])['samples']

    def _load_in_evapro(self, ena_samples):
        engine = create_engine("sqlite+pysqlite:///:memory:", future=True)
        with patch.object(self.loader, '_evapro_engine', side_effect=PropertyMock(return_value=engine)):
            metadata.create_all(engine)
            self.loader.begin_or_continue_transaction()
            analysis_obj = self.loader.insert_analysis('ERZ000001', 'title', 'ELOAD_3_AF1', 'description', 'centre',
                                                       datetime.datetime(2026, 10, 16), 1)
            analysis_obj.files.append(self.loader.insert_file('prj000001', 1, 1, 'af.vcf', 'md5', 'vcf', 10, 'ftp'))
            for ena_sample in ena_samples:
                self.loader.insert_sample(ena_sample['accession'], ena_sample['accession'])
            self.loader.eva_session.commit()
            # ENA associates the analysis with the samples sent in the ENA JSON
            self.loader.ena_project_finder = Mock(find_samples_in_ena=Mock(return_value=[
                (f'ERS{i}', ena_sample['accession']) for i, ena_sample in enumerate(ena_samples)
            ]))
            assert self.loader.load_samples_from_analysis(
                self.eload.sample_accession_to_name_for_analysis('AF1'), 'ERZ000001'
            )
            return sorted(
                (result.SampleInFile.sample.biosample_accession, result.SampleInFile.name_in_file)
                for result in self.loader.eva_session.execute(select(SampleInFile)).fetchall()
            )

    def assert_sample_tracking(self, samples, expected_ena_samples, expected_names_in_evapro):
        ena_samples = self._broker(samples)
        assert ena_samples == expected_ena_samples
        assert self._load_in_evapro(ena_samples) == expected_names_in_evapro

    def test_pre_registered_without_sample_in_vcf(self):
        # Used to collapse to a single accession: each sample must keep its own accession and get its BioSample name
        self.assert_sample_tracking(
            [{'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000001'},
             {'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000002', 'sampleInVCF': ''}],
            [{'accession': 'SAME000001'}, {'accession': 'SAME000002'}],
            [('SAME000001', 'bsd_name1'), ('SAME000002', 'bsd_name2')]
        )

    def test_pre_registered_and_new_with_and_without_sample_in_vcf(self):
        self.assert_sample_tracking(
            [{'analysisAlias': ['AF1'], 'bioSampleAccession': 'SAME000001'},
             self._new_sample('LH1'),
             self._new_sample('LH2', sample_in_vcf='POP2')],
            [{'accession': 'SAME000001'}, {'accession': 'SAMEA_NEW_LH1'},
             {'accession': 'SAMEA_NEW_LH2', 'alias': 'POP2'}],
            [('SAME000001', 'bsd_name1'), ('SAMEA_NEW_LH1', 'LH1'), ('SAMEA_NEW_LH2', 'POP2')]
        )
