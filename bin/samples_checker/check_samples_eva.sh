#!/usr/bin/env bash
NUM_ARGS=$#;
if [ ${NUM_ARGS} -ne 2 ]; then
    echo "Usage: check_samples_eva.sh <Path to metadata sheet> <Path to directory with the VCF files>";
    exit 1;
fi

CURR_SCRIPT_DIR="$( cd "$(dirname "$0")" ; pwd -P )";
cd ${CURR_SCRIPT_DIR}/../.. && git pull origin master && cd ${CURR_SCRIPT_DIR}/../../amp-t2d-submissions && git pull origin master && cd ${CURR_SCRIPT_DIR} && source venv/bin/activate && python check_samples_eva.py --metadata-file $1 --vcf-files-path $2;
