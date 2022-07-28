# Copyright 2020 EMBL - European Bioinformatics Institute
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

import os
from collections import defaultdict

from cached_property import cached_property
from ebi_eva_common_pyutils.logger import AppLogger

from eva_submission import ETC_DIR
from eva_submission.xlsx.xlsx_parser import XlsxReader, XlsxWriter


class EvaXlsxReader(AppLogger):

    def __init__(self, metadata_file):
        conf = os.path.join(ETC_DIR, 'eva_project_conf.yaml')
        self.reader = XlsxReader(metadata_file, conf)
        self.metadata_file=metadata_file

    def _get_all_rows(self, active_sheet):
        self.reader.active_worksheet = active_sheet
        return self.reader.get_rows()

    @cached_property
    def project(self):
        self.reader.active_worksheet = 'Project'
        try:
            return self.reader.next()
        except StopIteration:
            self.error('No project was found in the spreadsheet %s', self.metadata_file)

    @cached_property
    def submitters(self):
        return self._get_all_rows('Submitter Details')

    @cached_property
    def analysis(self):
        return self._get_all_rows('Analysis')

    @cached_property
    def samples(self):
        return self._get_all_rows('Sample')

    @cached_property
    def files(self):
        return self._get_all_rows('Files')

    @cached_property
    def project_title(self):
        if self.project:
            return self.project.get('Project Title')

    @property
    def analysis_titles(self):
        return [a.get('Analysis Title') for a in self.analysis]

    @property
    def references(self):
        return list(set([a.get('Reference') for a in self.analysis if a.get('Reference')]))

    @property
    def samples_per_analysis(self):
        samples_per_analysis = defaultdict(list)
        for row in self.samples:
            for analysis_alias in row.get('Analysis Alias').split(','):
                # remove white space between analysis
                samples_per_analysis[analysis_alias.strip()].append(row)
        return samples_per_analysis

    @property
    def files_per_analysis(self):
        files_per_analysis = defaultdict(list)
        for row in self.files:
            files_per_analysis[row.get('Analysis Alias')].append(row)
        return files_per_analysis


class EvaXlsxWriter(AppLogger):

    def __init__(self, metadata_source, metadata_dest=None):
        conf = os.path.join(ETC_DIR, 'eva_project_conf.yaml')
        self.writer = XlsxWriter(metadata_source, conf)
        self.metadata_source = metadata_source
        if metadata_dest:
            self.metadata_dest = metadata_dest
        else:
            self.metadata_dest = metadata_source

    def _set_all_rows(self, active_sheet, rows):
        self.writer.active_worksheet = active_sheet
        self.writer.set_rows(rows, empty_remaining_rows=True)

    def save(self):
        self.writer.save(self.metadata_dest)

    def set_files(self, file_dicts):
        self._set_all_rows('Files', file_dicts)

    def set_project(self, project_dict):
        self._set_all_rows('Project', [project_dict])

    def set_analysis(self, analysis_dicts):
        self._set_all_rows('Analysis', analysis_dicts)

    def set_samples(self, sample_dicts):
        self._set_all_rows('Sample', sample_dicts)

    def set_files(self, file_dicts):
        self._set_all_rows('Files', file_dicts)

