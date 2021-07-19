#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}
mkdir -p project project/accessions project/public ftp

# run accession and variant load
# note public_dir needs to be an absolute path, unlike others in config
printf "\e[32m===== ACCESSION PIPELINE =====\e[0m\n"
nextflow run "${SOURCE_DIR}/accession.nf" -params-file test_ingestion_config.yaml \
   --accessions_dir ${SCRIPT_DIR}/project/accessions \
	 --public_dir ${SCRIPT_DIR}/project/public
printf "\e[32m==== VARIANT LOAD PIPELINE ====\e[0m\n"
nextflow run ${SOURCE_DIR}/variant_load.nf -params-file test_ingestion_config.yaml \
   --project_dir ${SCRIPT_DIR}/project

# check for public files and logs
printf "\e[32m====== Files made public ======\e[0m\n"
ls project/public
printf "\n\e[32m======== Commands run ========\e[0m\n"
find work/ \( -name '*.out' -o -name '*.err' \) -exec cat {} \;

# clean up
rm -rf work .nextflow*
rm -r project ftp
cd ${cwd}
