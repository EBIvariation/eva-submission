#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

printf "\e[32m===== PREPARE BROKERING PIPELINE =====\e[0m\n"
nextflow run "${SOURCE_DIR}/prepare_brokering.nf" -params-file test_prepare_brokering_config.yaml

ls output/not_compressed.vcf.gz
ls output/test2.vcf.gz
ls output/not_compressed.vcf.gz.csi
ls output/not_compressed.vcf.gz.md5
ls output/not_compressed.vcf.gz.csi.md5

# clean up
rm -rf work .nextflow*
rm -r output
cd ${cwd}
