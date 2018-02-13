#!/bin/bash

# This script checks whether there are new uploaded VCFs and runs vcf-validator over them.
#
# uploading.txt: files that were modified in the last period between the previous execution of this script and this execution.
# files_to_validate.txt: files that were uploading, but were not modified in the last period. these will be validated.
# uploading_in_last_cron_execution.txt: files that were modified in the period before the last one.
# files_validated_in_last_cron_execution.txt: for human checks, the validation reports should be available for these VCFs.
# last_cron_execution.txt: empty file that is used as timestamp of the last execution of this script.

root_folder=/nfs/production3/eva/submissions/new_files_validations

find /nfs/ftp/private/eva-box-* -name "*vcf.gz" -o -name "*vcf" -newer ${root_folder}/last_cron_execution.txt > ${root_folder}/uploading.txt

touch ${root_folder}/files_to_validate.txt

while read line; do
  if grep -q "$line" ${root_folder}/uploading.txt; then
    # VCF pending validation but still uploading
    true # noop
  else
    # VCF was uploading, but not anymore: do validation
    echo "$line" >> ${root_folder}/files_to_validate.txt
  fi
done < ${root_folder}/uploading_in_last_cron_execution.txt


mutt -s "New VCFs ready for validation in FTP dropbox area" eva-dev@ebi.ac.uk < ${root_folder}/files_to_validate.txt


while read line; do
  validation_folder="${root_folder}/${line}.d/"
  mkdir -p "$validation_folder"
  bsub "zcat -f $line | /nfs/production3/eva/software/vcf-validator/version-history/vcf_validator_0.7_summary -r summary,text,database -o $validation_folder"
done < ${root_folder}/files_to_validate.txt

mv ${root_folder}/files_to_validate.txt ${root_folder}/files_validated_in_last_cron_execution.txt
mv ${root_folder}/uploading.txt ${root_folder}/uploading_in_last_cron_execution.txt

touch ${root_folder}/last_cron_execution.txt

