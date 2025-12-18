import json
import os

from eva_submission.eload_submission import Eload
from eva_submission.submission_config import EloadConfig


class EloadMetadataJsonLoader(Eload):

    def __init__(self, eload_number: int, config_object: EloadConfig = None):
        super().__init__(eload_number, config_object)
        self.metadata_json_path  = os.path.join(self._get_dir('ena'), 'metadata_json.json')
        if os.path.isfile(self.metadata_json_path):
            with open(self.metadata_json_path) as open_file:
                self.metadata_json = json.load(open_file)
        else:
            self.metadata_json = {}

    def get_experiment_types(self, analysis_accession):
        analysis_alias_dict = self.eload_cfg.query('brokering','ena','ANALYSIS')
        analysis_aliases = [a_alias for a_alias, a_accession in analysis_alias_dict.items() if a_accession == analysis_accession]
        if len(analysis_aliases) != 1:
            self.error(f'No experiment types can be found for {analysis_accession} accession')
            return []
        analysis_alias = self._unique_alias(analysis_aliases[0])

        analysis_json_dicts = [analysis_json
                               for analysis_json in self.metadata_json.get('analysis', {})
                               if analysis_json.get('analysisAlias') == analysis_alias]
        if analysis_json_dicts:
            # There can be only one experiment type
            return [analysis_json_dicts[0].get('experimentType',  '')]
        return []
