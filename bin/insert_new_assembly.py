#!/usr/bin/env python

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

import logging
from argparse import ArgumentParser

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_common_pyutils.metadata_utils import resolve_variant_warehouse_db_name, get_metadata_connection_handle

from eva_submission.assembly_taxonomy_insertion import insert_new_assembly_and_taxonomy
from eva_submission.eload_utils import provision_new_database_for_variant_warehouse
from eva_submission.submission_config import load_config

logger = log_cfg.get_logger(__name__)


def main():
    argparse = ArgumentParser(description='Insert an assembly with the provided name if it does not exist already. '
                                          'Associate it with a taxonomy that will be inserted as well if it does not '
                                          'exist')
    argparse.add_argument('--assembly_accession', required=True, type=str, help='The assembly accession to add')
    argparse.add_argument('--taxonomy_id', required=True, type=int, help='The taxonomy id to associate with the '
                                                                         'assembly')
    argparse.add_argument('--debug', action='store_true', default=False,
                          help='Set the script to output logging information at debug level')

    log_cfg.add_stdout_handler()
    args = argparse.parse_args()
    if args.debug:
        log_cfg.set_log_level(logging.DEBUG)

    # Load the config_file from default location
    load_config()

    assembly_accession = args.assembly_accession
    taxon_id = args.taxonomy_id
    with get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file']) as conn:
        db_name = resolve_variant_warehouse_db_name(conn, assembly=assembly_accession, taxonomy=taxon_id)
        if not db_name:
            raise ValueError(f'Database name for taxid:{taxon_id} and assembly {assembly_accession} '
                             f'could not be retrieved or constructed')
        # warns but doesn't crash if assembly set already exists
        insert_new_assembly_and_taxonomy(
            assembly_accession=assembly_accession,
            taxonomy_id=taxon_id,
            db_name=db_name,
            conn=conn
        )


if __name__ == "__main__":
    main()
