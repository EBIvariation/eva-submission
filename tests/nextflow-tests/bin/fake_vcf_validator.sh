#!/bin/bash

echo "vcf_validator $*"

filename=$2
touch vcf_format/$filename.errors.1.db
touch vcf_format/$filename.errors.1.txt
touch vcf_format/$filename.vcf_format.log
