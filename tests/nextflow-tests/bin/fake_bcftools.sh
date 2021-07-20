#!/bin/bash

echo "bcftools $*"

if [[ $1 == "merge" ]]; then
    filename=${*: -1}
    touch $filename
    printf "> Files merged:\n"
    cat $5
    printf "\n"
elif [[ $1 == "sort" ]]; then
    filename=${*: -2}
    touch $filename
else
    filename=$3
    touch ${filename}.csi
fi
