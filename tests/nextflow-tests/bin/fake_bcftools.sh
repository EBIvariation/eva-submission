#!/bin/bash

echo "bcftools $*"

if [[ $1 == "merge" ]]; then
    filename=${*: -1}
    touch $filename
    printf "> Files merged:\n"
    cat all_files.list
    printf "\n"
else
    filename=$3
    touch ${filename}.csi
fi
