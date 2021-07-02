#!/usr/bin/env python
import os

import yaml
from ebi_eva_common_pyutils.config import Configuration, cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission import __version__

logger = log_cfg.get_logger(__name__)


class EloadConfig(Configuration):
    """Configuration object that allows write to the config file"""

    def load_config_file(self, *search_path):
        try:
            super().load_config_file(*search_path)
        except FileNotFoundError:
            # expected if it the first time we create the config file
            # in that case the first search path is set to be the config files
            self.config_file = search_path[0]
            pass

    # TODO where do we call this?
    def upgrade_if_needed(self):
        """
        Upgrades unversioned configs (i.e. pre-1.0) to the current version.
        Currently doesn't perform any other version upgrades.
        """
        if 'version' not in self.content:
            logger.info(f'No version found in config, upgrading to version {__version__}.')

            self.set('version', value=__version__)
            if 'submission' not in self.content:
                logger.error('Need submission config section to upgrade')
                logger.error('Try running prepare_backlog_study.py to build a config from scratch.')
                raise ValueError('Need submission config section to upgrade')

            # Note: if we're converting an old config, there's only one analysis
            # TODO get alias from metadata? does this need to be the actual analysis alias???
            analysis_alias = 'Analysis 1'
            analysis_data = {
                'assembly_accession': self.pop('submission', 'assembly_accession'),
                'assembly_fasta': self.pop('submission', 'assembly_fasta'),
                'assembly_report': self.pop('submission', 'assembly_report'),
                'vcf_files': self.pop('submission', 'vcf_files')
            }
            analysis_dict = {analysis_alias: analysis_data}
            self.set('submission', 'analyses', value=analysis_dict)

            if 'validation' in self.content:
                self.pop('validation', 'valid', 'vcf_files')
                self.set('validation', 'valid', 'analyses', value=analysis_dict)

            if 'brokering' in self.content:
                brokering_vcfs = {
                    vcf_file: index_dict
                    for vcf_file, index_dict in self.pop('brokering', 'vcf_files').items()
                }
                analysis_dict[analysis_alias]['vcf_files'] = brokering_vcfs
                self.set('brokering', 'analyses', value=analysis_dict)
                analysis_accession = self.query('brokering', 'ena', 'ANALYSIS')
                self.set('brokering', 'ena', 'ANALYSIS', analysis_alias, value=analysis_accession)

        else:
            # TODO think through how complicated this might get...
            logger.info(f"Config is version {self.query('version')}, not upgrading.")

    def write(self):
        if self.config_file and self.content and os.path.isdir(os.path.dirname(self.config_file)):
            with open(self.config_file, 'w') as open_config:
                yaml.safe_dump(self.content, open_config)

    def set(self, *path, value):
        top_level = self.content
        for p in path[:-1]:
            if p not in top_level:
                top_level[p] = {}
            top_level = top_level[p]
        top_level[path[-1]] = value

    def pop(self, *path, default=None):
        """Recursive dictionary pop with default"""
        top_level = self.content
        for p in path[:-1]:
            if p not in top_level:
                return default
            top_level = top_level[p]
        return top_level.pop(path[-1], default)

    def is_empty(self):
        return not self.content

    def clear(self):
        self.content = {}

    def __setitem__(self, item, value):
        """Allow dict-style write access, e.g. config['this']='that'."""
        # If we're starting to fill in an empty config, set the version.
        if self.is_empty():
            self.content['version'] = __version__
        self.content[item] = value

    def __del__(self):
        self.write()


def load_config(*args):
    """Load a config file from any path provided.
    If none are provided then read from a file path provided in the environment variable SUBMISSIONCONFIG.
    If not provided then default to .submission_config.yml place in the current users' home"""
    cfg.load_config_file(
        *args,
        os.getenv('SUBMISSIONCONFIG'),
        os.path.expanduser('~/.submission_config.yml'),
    )
