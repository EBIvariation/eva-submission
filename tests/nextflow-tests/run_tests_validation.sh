#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

printf "\e[32m===== VALIDATION PIPELINE =====\e[0m\n"
nextflow run "${SOURCE_DIR}/validation.nf" -params-file test_validation_config.yaml --eva_sub_cli_validation_dir "${SCRIPT_DIR}/cli_validation_dir"

ls output/sv_check/test1.vcf_sv_check.log \
output/sv_check/test1.vcf_sv_list.vcf.gz \
cli_validation_dir/validation_results.yaml \
cli_validation_dir/validation_output/report.html \
cli_validation_dir/validation_output/report.txt


# clean up
rm -rf work .nextflow*
rm -r output cli_validation_dir
cd ${cwd}
