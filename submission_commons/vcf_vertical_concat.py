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


class VerticalConcatProcess:
    def __init__(self, files_to_concat_list: str, concat_processing_dir: str, output_vcf_file: str,
                 bcftools_binary: str = "bcftools"):
        self.files_to_concat_list = files_to_concat_list
        self.concat_processing_dir = concat_processing_dir
        self.output_vcf_file = output_vcf_file
        self.bcftools_binary = bcftools_binary

    def vertical_concat(self):
        os.makedirs(os.path.dirname(self.output_vcf_file), exist_ok=True)
        run_command_with_output(f"Running bcftools concat with the file list {self.files_to_concat_list}...",
                                f"{self.bcftools_binary} concat "
                                "--allow-overlaps --remove-duplicates "
                                f"--file-list {self.files_to_concat_list} -o {self.output_vcf_file} -O z")
        # Use CSI indexes because they can support longer genomes
        # see http://www.htslib.org/doc/tabix.html
        run_command_with_output(f"Running tabix on the output VCF {self.output_vcf_file}...",
                                f"{self.bcftools_binary} index --csi {self.output_vcf_file}")


def vcf_vertical_concat(files_to_concat_list, concat_processing_dir, output_vcf_file, bcftools_binary):
    concat_process = VerticalConcatProcess(files_to_concat_list=files_to_concat_list,
                                           concat_processing_dir=concat_processing_dir, output_vcf_file=output_vcf_file,
                                           bcftools_binary=bcftools_binary)
    concat_process.vertical_concat()


def main():
    parser = argparse.ArgumentParser(description='Vertically concatenate multiple VCF files',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--files-to-concat-list",
                        help="Text file containing the list of VCF files to concatenate", required=True)
    parser.add_argument("--concat-processing-dir",
                        help="Full path to the directory that should contain the concatenation output", required=True)
    parser.add_argument("--output-vcf-file",
                        help="Full path to the concatenation output file", required=True)
    parser.add_argument("--bcftools-binary",
                        help="Full path to the binary for bcftools", default="bcftools", required=False)
    args = parser.parse_args()
    vcf_vertical_concat(args.files_to_concat_list, args.concat_processing_dir, args.output_vcf_file,
                        args.bcftools_binary)


if __name__ == "__main__":
    main()
