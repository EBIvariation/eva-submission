#!/bin/bash

>&2 echo "bcftools $*"
command=$1
shift

OTHER_ARGS=()

while [[ $# -gt 0 ]]; do
  case $1 in
    -o)
      filename="$2"
      shift # argument
      shift # value
      ;;
    *)
      OTHER_ARGS+=("$1") # save other arg
      shift # argument
      ;;
  esac
done

>&2 echo "${OTHER_ARGS[*]}"

if [[ $command == "merge" ]]; then
    if [ -z $filename ]; then
      filename=${OTHER_ARGS: -1}
    fi
    printf "> Files merged:\n"
elif [[ $command == "sort" ]]; then
    # Only create the output if it is specified
    if [ ! -z $filename ]; then
      touch $filename
    fi
elif [[ $command == "norm" ]]; then
    if [ ! -z $filename ]; then
      touch $filename
    fi
else
    if [ -z $filename ]; then
      filename=${OTHER_ARGS[1]}
    fi
    touch ${filename}.csi
fi
