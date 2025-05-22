#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

printf "\e[32m===== VALIDATION PIPELINE =====\e[0m\n"
nextflow run "${SOURCE_DIR}/validation.nf" -params-file test_validation_config.yaml

ls output/sv_check/test1.vcf_sv_check.log \
output/sv_check/test1.vcf_sv_list.vcf.gz \
output/assembly_check/test1.vcf.gz.assembly_check.log \
output/assembly_check/test1.vcf.gz.text_assembly_report \
output/assembly_check/test1.vcf.gz.valid_assembly_report \
output/validation_results.yaml \
output/validation_output/report.html


# clean up
rm -rf work .nextflow*
rm -r output
cd ${cwd}
