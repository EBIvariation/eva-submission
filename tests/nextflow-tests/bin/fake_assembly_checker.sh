#!/bin/bash

echo "assembly_checker $*"

filename=$2
touch assembly_check/$filename.valid_assembly_report
touch assembly_check/$filename.text_assembly_report
touch assembly_check/$filename.assembly_check.log

# just to test the exit code is accepted
exit 139
