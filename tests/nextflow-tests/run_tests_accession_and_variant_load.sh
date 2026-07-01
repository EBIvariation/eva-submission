#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}
mkdir -p project project/accessions project/public project/logs ftp

# clusterOptions (and so the -o/-e logs it configures) are only honoured by grid executors,
# not by the local executor used when running these tests outside of CI
USING_SLURM=false
if grep -q "executor = 'slurm'" nextflow.config; then
    USING_SLURM=true
fi

# run accession and variant load
# note public_dir needs to be an absolute path, unlike others in config
printf "\e[32m==== ACCESSION & VARIANT LOAD PIPELINES ====\e[0m\n"
nextflow run ${SOURCE_DIR}/accession_and_load.nf -params-file test_ingestion_config.yaml \
   --project_dir ${SCRIPT_DIR}/project \
   --accessions_dir ${SCRIPT_DIR}/project/accessions \
	 --public_dir ${SCRIPT_DIR}/project/public
# check for public files and logs
printf "\e[32m====== Files made public ======\e[0m\n"
for f in test1 test2 test3; do ls project/public/${f}.accessioned.vcf.gz project/public/${f}.accessioned.vcf.gz.csi; done
ls -1 project/public/ | wc -l

if [ "${USING_SLURM}" = true ]; then
    # check that slurm actually submitted the accessioning jobs and wrote their -o/-e clusterOptions logs
    printf "\n\e[32m======== Slurm accessioning logs ========\e[0m\n"
    for f in test1 test2 test3; do ls project/logs/accessioning.${f}.vcf.gz.log project/logs/accessioning.${f}.vcf.gz.err; done
fi

printf "\n\e[32m======== Commands run ========\e[0m\n"
find work/ \( -name '*.out' -o -name '*.err' \) -exec cat {} \;

# clean up
rm -rf work .nextflow*
rm -r project ftp
cd ${cwd}
