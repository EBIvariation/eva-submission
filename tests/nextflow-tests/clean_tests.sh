#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

rm -rf work .nextflow*
rm -r project ftp
cd ${cwd}
