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
import glob
import inspect
import math
import os
import sys

from ebi_eva_common_pyutils.logger import logging_config
from ebi_eva_common_pyutils.nextflow import NextFlowPipeline, NextFlowProcess
from submission_commons import vcf_vertical_concat

logger = logging_config.get_logger(__name__)


def get_python_process_command_string(python_program, args: dict, log_file: str):
    """
    Helper class to generate the command to run a Python program with command-line arguments
    """
    program_module_dir = os.path.dirname(inspect.getmodule(python_program).__file__)
    program_name = python_program.__name__.split(".")[-1]
    args_repr = " ".join([f"--{arg} {val}" for arg, val in args.items()])
    return f"bash -c \"export PYTHONPATH='{program_module_dir}' && " \
           f"{sys.executable} -m {program_name} {args_repr}\" 1>> {log_file} 2>&1"


def get_multistage_vertical_concat_pipeline(vcf_files, concat_processing_dir, concat_chunk_size, bcftools_binary,
                                            stage=0, prev_stage_processes=[], pipeline=NextFlowPipeline()):
    """
    # Generate Nextflow pipeline for multi-stage VCF concatenation of 5 VCF files with 2-VCFs concatenated at a time (CONCAT_CHUNK_SIZE=2)
    # For illustration purposes only. Usually the CONCAT_CHUNK_SIZE is much higher (ex: 500).
    #
    #		    vcf1		    vcf2		vcf3		    vcf4		vcf5
    #               \		     /		       \		      /
    # Stage0:	     \		   /		        \		    /
    # -------	      \	     /		             \	      /
    #	    	vcf1_2=concat(vcf1,vcf2)	vcf3_4=concat(vcf3,vcf4)	vcf5    <---- 3 batches of concat in stage 0
    #		 		    \ 		                /
    # Stage1:	  		 \		              /
    # -------	   		  \	                /
    #				vcf1_2_3_4=concat(vcf1_2,vcf3_4)		            vcf5    <---- 2 batches of concat in stage 1
    #	 					      \ 		                            /
    # Stage2:	  		 		   \		 	                      /
    # -------	   		  			\	                            /
    #						      vcf1_2_3_4_5=concat(vcf1_2_3_4,vcf5)          <----- Final result
    """
    if len(vcf_files) == 1: # If we are left with only one file, this means we have reached the last concat stage
        return pipeline, vcf_files[0]
    num_batches_in_stage = math.ceil(len(vcf_files) / concat_chunk_size)
    curr_stage_processes = []
    output_vcf_files_from_stage = []
    for batch in range(0, num_batches_in_stage):
        # split files in the current stage into chunks based on concat_chunk_size
        files_in_batch = vcf_files[(concat_chunk_size * batch):(concat_chunk_size * (batch + 1))]
        files_to_concat_list = write_files_to_concat_list(files_in_batch, stage, batch, concat_processing_dir)
        concat_stage_batch_name = f"concat_stage{stage}_batch{batch}"
        log_file_name = os.path.join(concat_processing_dir, f"{concat_stage_batch_name}.log")
        output_vcf_file = get_output_vcf_file_name(stage, batch, concat_processing_dir)
        process = NextFlowProcess(process_name=concat_stage_batch_name,
                                  command_to_run=str(get_python_process_command_string(
                                      vcf_vertical_concat,
                                      {"files-to-concat-list": files_to_concat_list,
                                       "concat-processing-dir": concat_processing_dir,
                                       "output-vcf-file": output_vcf_file,
                                       "bcftools-binary": bcftools_binary},
                                      log_file=log_file_name
                                  )
                                  )
                                  )
        curr_stage_processes.append(process)
        output_vcf_files_from_stage.append(output_vcf_file)
        # Concatenation batch in a given stage will have to wait until the completion of
        # n batches in the previous stage where n = concat_chunk_size
        # Ex: In the illustration above stage 1/batch 0 depends on completion of stage 0/batch 0 and stage 0/batch 1
        # While output of any n batches from the previous stage can be worked on as they become available,
        # having a predictable formula simplifies pipeline generation and troubleshooting
        prev_stage_dependencies = prev_stage_processes[(concat_chunk_size * batch):(concat_chunk_size * (batch + 1))]
        pipeline.add_dependencies({process: prev_stage_dependencies})
    prev_stage_processes = curr_stage_processes
    return get_multistage_vertical_concat_pipeline(output_vcf_files_from_stage,
                                                   concat_processing_dir, concat_chunk_size,
                                                   bcftools_binary,
                                                   stage=stage+1, prev_stage_processes=prev_stage_processes,
                                                   pipeline=pipeline)


def write_files_to_concat_list(files_to_concat, concat_stage, concat_batch, concat_processing_dir):
    """
    Write the list of files to be concatenated for a given stage and batch
    """
    files_to_concat_list = os.path.join(get_concat_output_dir(concat_stage, concat_processing_dir),
                                        f"batch{concat_batch}_files_to_be_concatenated.txt")
    os.makedirs(os.path.dirname(files_to_concat_list), exist_ok=True)
    with open(files_to_concat_list, "w") as handle:
        for filename in files_to_concat:
            handle.write(filename + "\n")

    return files_to_concat_list


def get_concat_output_dir(concat_stage_index: int, concat_processing_dir: str):
    """
    Get the file name with the list of files to be concatenated for a given stage and batch in the concatenation process
    """
    return os.path.join(concat_processing_dir, "vertical_concat", f"stage_{concat_stage_index}")


def get_output_vcf_file_name(concat_stage_index: int, concat_batch_index: int, concat_processing_dir: str):
    return os.path.join(get_concat_output_dir(concat_stage_index, concat_processing_dir),
                        f"concat_output_stage{concat_stage_index}_batch{concat_batch_index}.vcf.gz")


def run_vcf_vertical_concat_pipeline(toplevel_vcf_dir, concat_processing_dir, concat_chunk_size,
                                     bcftools_binary, nextflow_binary, nextflow_config_file, resume):
    vcf_files = sorted(glob.glob(f"{toplevel_vcf_dir}/**/*.vcf.gz", recursive=True))
    pipeline, concat_result_file = get_multistage_vertical_concat_pipeline(vcf_files, concat_processing_dir,
                                                                           concat_chunk_size, bcftools_binary)
    pipeline.run_pipeline(workflow_file_path=os.path.join(concat_processing_dir, "vertical_concat.nf"),
                          nextflow_binary_path=nextflow_binary, nextflow_config_path=nextflow_config_file,
                          resume=resume)
    logger.info(f"Concatenated output file is in: {concat_result_file}")


def main():
    parser = argparse.ArgumentParser(description='Vertically concatenate multiple VCF files in several stages',
                                     formatter_class=argparse.RawTextHelpFormatter, add_help=False)
    parser.add_argument("--toplevel-vcf-dir",
                        help="Full path to the directory which contains all the VCF files "
                             "(either at the top-level or within sub-directories)", required=True)
    parser.add_argument("--concat-processing-dir",
                        help="Full path to the directory that should contain the concatenation output", required=True)
    parser.add_argument("--concat-chunk-size",
                        help="Maximum number of files that can be concatenated in a given batch", type=int,
                        default=500, required=False)
    parser.add_argument("--bcftools-binary",
                        help="Full path to the binary for bcftools", default="bcftools", required=False)
    parser.add_argument("--nextflow-binary",
                        help="Full path to the binary for Nextflow", default="nextflow", required=False)
    parser.add_argument("--nextflow-config-file",
                        help="Full path to the Nextflow config file", default=None, required=False)
    parser.add_argument("--resume",
                        help="Indicate if a previous concatenation job is to be resumed", action='store_true',
                        required=False)
    args = parser.parse_args()
    run_vcf_vertical_concat_pipeline(**vars(args))


if __name__ == "__main__":
    main()
