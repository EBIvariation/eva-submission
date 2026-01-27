#!/usr/bin/env python

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

import argparse
import os

from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_internal_pyutils.spring_properties import SpringPropertiesGenerator

from eva_submission.submission_config import load_config


def create_properties(maven_profile, private_settings_file, assembly_accession=None, deprecation_suffix=None, deprecation_reason=None,
                      ssid_variant_file=None, chunk_size=100):
    """Properties for deprecation pipeline."""
    properties_generator = SpringPropertiesGenerator(maven_profile, private_settings_file)
    return properties_generator._format(
        properties_generator._common_accessioning_clustering_properties(
            assembly_accession=assembly_accession, read_preference='secondaryPreferred', chunk_size=chunk_size
        ),
        {
            'spring.batch.job.names': 'DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_JOB',
            'parameters.deprecationIdSuffix': deprecation_suffix,
            'parameters.deprecationReason': deprecation_reason,
            'parameters.variantIdFile': ssid_variant_file
        }
    )

def main():
    parser = argparse.ArgumentParser(description="Deprecate ssids provided in the input file")
    parser.add_argument("--variant_id_file", help="A single column file containing the ssids to deprecate")
    parser.add_argument("--assembly_accession", help="Assembly within which the ssids should be deprecated")
    parser.add_argument("--deprecation_suffix", help="Text used for to annotate operation Id after the keyword SS_DEPRECATED_ and before the actual ID")
    parser.add_argument("--deprecation_reason", help="Text used to provide the reason of the deprecation.")
    parser.add_argument("--ssid_variant_file", help="File containing the ")
    args = parser.parse_args()

    # Load the config_file from default location
    load_config()

    private_settings_file = cfg['maven']['settings_file']
    maven_profile = cfg['maven']['environment']
    property_text = create_properties(
        maven_profile, private_settings_file, assembly_accession=args.assembly_accession,
        deprecation_suffix=args.deprecation_suffix, deprecation_reason=args.deprecation_reason,
        ssid_variant_file=args.variant_id_file
    )
    filename, ext = os.path.splitext(args.variant_id_file)

    properties_file = filename + '_' + args.assembly_accession + '_deprecation.properties'
    deprecation_log = filename + '_' + args.assembly_accession + '_deprecation.log'
    with open(properties_file, 'w') as open_file:
        open_file.write(property_text)

    command = f'java -jar {cfg["jar"]["accession_pipeline"]} --spring.config.location=file:{properties_file} > {deprecation_log} 2>&1'
    run_command_with_output(f"Run the deprecated on {args.variant_id_file}", command)

