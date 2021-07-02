#!/usr/bin/env python
import os

import yaml
from ebi_eva_common_pyutils.config import Configuration, cfg

from eva_submission import __version__


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
