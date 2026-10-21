from unittest import TestCase

from eva_submission.biosample_submission.sample_record import SampleRecord, records_to_dicts, records_from_dicts


class TestSampleRecord(TestCase):

    def test_normalisation(self):
        record = SampleRecord('SAME000001', analysis_aliases=('AF1',), sample_in_vcf='  ', name='', derived_from=[],
                              action='')
        assert record == SampleRecord('SAME000001', analysis_aliases=['AF1'])
        assert record.sample_in_vcf is None
        assert record.name is None
        assert record.derived_from is None
        assert record.action is None

    def test_to_dict_only_contains_set_fields(self):
        assert SampleRecord('SAME000001', analysis_aliases=['AF1']).to_dict() == {
            'accession': 'SAME000001', 'analysisAlias': ['AF1']
        }
        assert SampleRecord('SAME000002', analysis_aliases=['GT1'], sample_in_vcf='S1', name='bsd',
                            derived_from=['SAME000001'], action='derive').to_dict() == {
            'accession': 'SAME000002', 'analysisAlias': ['GT1'], 'sampleInVCF': 'S1', 'name': 'bsd',
            'derivedFrom': ['SAME000001'], 'action': 'derive'
        }

    def test_round_trip(self):
        records = [
            SampleRecord('SAME000001', analysis_aliases=['AF1'], name='bsd1', action='None'),
            SampleRecord('SAME000002', analysis_aliases=['GT1'], sample_in_vcf='S1', derived_from=['SAME000003'],
                         action='derive'),
            SampleRecord.from_legacy('S2', 'SAME000004'),
        ]
        assert records_from_dicts(records_to_dicts(records)) == records

    def test_from_legacy(self):
        record = SampleRecord.from_legacy('S1', 'SAME000001')
        assert record.analysis_aliases is None
        assert record.to_dict() == {'accession': 'SAME000001', 'analysisAlias': None, 'sampleInVCF': 'S1'}

    def test_label(self):
        assert SampleRecord('SAME000001', sample_in_vcf='S1', name='bsd').label == 'S1'
        assert SampleRecord('SAME000001', name='bsd').label == 'bsd'
        assert SampleRecord('SAME000001').label == 'SAME000001'

    def test_modified_and_derived(self):
        assert SampleRecord('SAME000001', action='create').modified
        assert not SampleRecord('SAME000001', action='None').modified
        assert not SampleRecord('SAME000001').modified
        assert SampleRecord('SAME000002', derived_from=['SAME000001']).is_derived
        assert not SampleRecord('SAME000001').is_derived

    def test_in_analysis(self):
        record = SampleRecord('SAME000001', analysis_aliases=['ELOAD_1_GT1'])
        assert record.in_analysis('ELOAD_1_GT1')
        assert not record.in_analysis('GT1')
        assert record.in_analysis('GT1', normalise_alias=lambda alias: alias.replace('ELOAD_1_', ''))
        # Records without analysis information belong to all analyses
        assert SampleRecord.from_legacy('S1', 'SAME000001').in_analysis('GT1')
        assert SampleRecord('SAME000001', analysis_aliases=[]).in_analysis('GT1')
