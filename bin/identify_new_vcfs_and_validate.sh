#!/bin/bash

find /nfs/ftp/private/eva-box-* -name "*vcf.gz" -newer /nfs/production3/eva/submissions/new_files_validations/last_cron_execution.txt > /nfs/production3/eva/submissions/new_files_validations/uploading.txt

while read line; do
  if grep -q "$line" /nfs/production3/eva/submissions/new_files_validations/uploading.txt; then
    # file in both files, this means it's still uploading
    true # noop
  else
    # file was uploading, but not anymore: do validation
    echo "$line" >> /nfs/production3/eva/submissions/new_files_validations/files_to_validate.txt
  fi
done < /nfs/production3/eva/submissions/new_files_validations/was_uploading_in_last_cron_execution.txt


mutt -s "New VCFs for validation in FTP dropbox area" garys@ebi.ac.uk jmmut@ebi.ac.uk < /nfs/production3/eva/submissions/new_files_validations/files_to_validate.txt

while read line; do
  validation_folder="/nfs/production3/eva/submissions/new_files_validations/${line}.d/"
  mkdir -p "$validation_folder"
  bsub "zcat $line | /nfs/production3/eva/software/vcf-validator/version-history/vcf_validator_0.7_summary -r summary,text,database -o $validation_folder"
done < /nfs/production3/eva/submissions/new_files_validations/files_to_validate.txt

mv /nfs/production3/eva/submissions/new_files_validations/files_to_validate.txt /nfs/production3/eva/submissions/new_files_validations/files_validated_in_last_cron_execution.txt
mv /nfs/production3/eva/submissions/new_files_validations/uploading.txt /nfs/production3/eva/submissions/new_files_validations/was_uploading_in_last_cron_execution.txt

touch /nfs/production3/eva/submissions/new_files_validations/last_cron_execution.txt

