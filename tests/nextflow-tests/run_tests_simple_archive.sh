#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}
mkdir -p project/public ftp
for f in test1 test2 test3; do if [ ! -f project/public/${f}.vcf.gz ]; then ln vcfs/${f}.vcf.gz project/public/;  fi ; done

# note public_dir needs to be an absolute path, unlike others in config
printf "\e[32m==== Copy to FTP ====\e[0m\n"
nextflow run ${SOURCE_DIR}/simple_archive.nf -params-file test_ingestion_config.yaml \
	 --public_dir ${SCRIPT_DIR}/project/public
# check for public files and logs
printf "\e[32m====== Files made public ======\e[0m\n"
for f in test1 test2 test3; do ls project/public/${f}.vcf.gz; done
for f in test1 test2 test3; do ls ftp/PRJEB12345/${f}.vcf.gz; done
ls -1 project/public/ | wc -l
ls -1 ftp/PRJEB12345/ | wc -l
printf "\n\e[32m======== Commands run ========\e[0m\n"
find work/ \( -name '*.out' -o -name '*.err' \) -exec cat {} \;

# clean up
rm -rf work .nextflow*
rm -r project ftp
cd ${cwd}
