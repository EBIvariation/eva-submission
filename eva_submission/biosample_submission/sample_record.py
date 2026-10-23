# Copyright 2026 EMBL - European Bioinformatics Institute
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from dataclasses import dataclass
from typing import List, Optional

ACCESSION_KEY = 'accession'
ANALYSIS_ALIAS_KEY = 'analysisAlias'
SAMPLE_IN_VCF_KEY = 'sampleInVCF'
NAME_KEY = 'name'
DERIVED_FROM_KEY = 'derivedFrom'
ACTION_KEY = 'action'

# Action returned by the BioSamples submitter when the sample was only retrieved
NO_ACTION = 'None'


def normalise_sample_in_vcf(sample_in_vcf):
    """Return the sample name in VCF or None when it is missing, null, empty or only whitespace."""
    if sample_in_vcf is None or not str(sample_in_vcf).strip():
        return None
    return sample_in_vcf


@dataclass
class SampleRecord:
    """
    Tracks one input sample: its accession, the analyses it belongs to and, when known, its name in the VCF and its
    BioSample name.
    """
    accession: Optional[str]
    analysis_aliases: Optional[List[str]] = None
    sample_in_vcf: Optional[str] = None
    name: Optional[str] = None
    derived_from: Optional[List[str]] = None
    action: Optional[str] = None

    def __post_init__(self):
        self.analysis_aliases = list(self.analysis_aliases or [])
        self.sample_in_vcf = normalise_sample_in_vcf(self.sample_in_vcf)
        self.name = self.name or None
        self.derived_from = list(self.derived_from) if self.derived_from else None
        self.action = self.action or None

    @classmethod
    def from_dict(cls, record_dict):
        return cls(
            record_dict.get(ACCESSION_KEY),
            analysis_aliases=record_dict.get(ANALYSIS_ALIAS_KEY),
            sample_in_vcf=record_dict.get(SAMPLE_IN_VCF_KEY),
            name=record_dict.get(NAME_KEY),
            derived_from=record_dict.get(DERIVED_FROM_KEY),
            action=record_dict.get(ACTION_KEY)
        )

    def to_dict(self):
        """Representation stored in the ELOAD config: optional fields are only present when set."""
        record_dict = {ACCESSION_KEY: self.accession, ANALYSIS_ALIAS_KEY: self.analysis_aliases}
        if self.sample_in_vcf:
            record_dict[SAMPLE_IN_VCF_KEY] = self.sample_in_vcf
        if self.name:
            record_dict[NAME_KEY] = self.name
        if self.derived_from:
            record_dict[DERIVED_FROM_KEY] = list(self.derived_from)
        if self.action:
            record_dict[ACTION_KEY] = self.action
        return record_dict

    @property
    def label(self):
        """Name used for this sample when no name in VCF is required: name in VCF, else BioSample name, else accession."""
        return self.sample_in_vcf or self.name or self.accession

    @property
    def is_derived(self):
        return bool(self.derived_from)

    @property
    def modified(self):
        """True when the BioSample was created or changed, False when it was only retrieved."""
        return self.action not in (None, NO_ACTION)

    def in_analysis(self, analysis_alias, normalise_alias=None):
        """
        Whether this record belongs to the analysis.
        normalise_alias is applied to both sides before comparison (e.g. to compare unique aliases).
        """
        normalise_alias = normalise_alias or (lambda alias: alias)
        return normalise_alias(analysis_alias) in [normalise_alias(alias) for alias in self.analysis_aliases]


def records_to_dicts(sample_records):
    return [record.to_dict() for record in sample_records]


def records_from_dicts(record_dicts):
    return [SampleRecord.from_dict(record_dict) for record_dict in record_dicts]
