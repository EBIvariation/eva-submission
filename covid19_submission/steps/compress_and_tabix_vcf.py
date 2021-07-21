# Copyright 2021 EMBL - European Bioinformatics Institute
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
from ebi_eva_common_pyutils.logger import logging_config
from subprocess import CalledProcessError

logger = logging_config.get_logger(__name__)
logging_config.add_stdout_handler()


def run_vcf_validation(vcf_file: str, validator_binary: str, output_dir: str) -> None:
    os.makedirs(name=output_dir, exist_ok=True)
    validation_output_prefix = os.path.basename(vcf_file)
    # This log file captures the status of the overall validation process
    process_log_file_name = f"{output_dir}/{validation_output_prefix}.vcf_format.log"
    try:
        run_command_with_output(f"Validating VCF file {vcf_file}...",
                                f'bash -c "{validator_binary} -i {vcf_file}  '
                                f'-r database,text '
                                f'-o {output_dir} '
                                f'--require-evidence > {process_log_file_name} 2>&1"')
    except CalledProcessError:
        last_generated_validator_output_file = run_command_with_output(
            f"Finding last updated validation output for {vcf_file}...",
            f'bash -c "ls -1rt {output_dir}/{validation_output_prefix}.errors.*.txt | tail -1"',
            return_process_output=True).strip()
        assert last_generated_validator_output_file, f"FAIL: Could not find validator output for file!!: {vcf_file}"
        # Since the VCF validator output is timestamped,
        # examine the last generated validator output file (race conditions galore!!)
        # TODO: Currently the Covid-19 DP submissions are in VCFv4.0 and therefore generate these errors
        #       even though they don't create any issues with the accessioning process.
        #       Therefore, we have decided to ignore these errors.
        #       Re-visit this code if Covid-19 DP VCF file formats are changed to v4.1.
        number_of_unacceptable_errors = int(run_command_with_output(
            f"Checking if VCF file {vcf_file} has acceptable errors...",
            f'bash -c "grep -i -v -E '
            f"'fileformat declaration is not valid|input file is not valid' {last_generated_validator_output_file} "
            '| wc -l"',
            return_process_output=True))
        if number_of_unacceptable_errors > 0:
            logger.error(f"Unacceptable VCF validation errors found. "
                         f"See file {last_generated_validator_output_file} for details.")
            raise SystemExit("Unacceptable VCF validation errors found!")


def main():
    parser = argparse.ArgumentParser(description='Validate a VCF file', formatter_class=argparse.RawTextHelpFormatter,
                                     add_help=False)
    parser.add_argument("--vcf-file", help="Full path to the VCF file", required=True)
    parser.add_argument("--validator-binary", help="Full path to the VCF validator binary",
                        default="vcf_validator", required=False)
    parser.add_argument("--output-dir", help="Full path to the validation output directory", required=True)

    args = parser.parse_args()
    run_vcf_validation(args.vcf_file, args.validator_binary, args.output_dir)


if __name__ == "__main__":
    main()
