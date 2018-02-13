#!/bin/bash

main_folder=/nfs/production3/eva/submissions/new_files_validations

find /nfs/ftp/private/eva-box-* -name "*vcf.gz" -newer ${main_folder}/last_cron_execution.txt > ${main_folder}/uploading.txt
find /nfs/ftp/private/eva-box-* -name "*vcf" -newer ${main_folder}/last_cron_execution.txt >> ${main_folder}/uploading.txt

touch ${main_folder}/files_to_validate.txt

while read line; do
  if grep -q "$line" ${main_folder}/uploading.txt; then
    # file in both files, this means it's still uploading
    true # noop
  else
    # file was uploading, but not anymore: do validation
    echo "$line" >> ${main_folder}/files_to_validate.txt
  fi
done < ${main_folder}/was_uploading_in_last_cron_execution.txt


mutt -s "New VCFs for validation in FTP dropbox area" eva-dev@ebi.ac.uk < ${main_folder}/files_to_validate.txt


while read line; do
  validation_folder="${main_folder}/${line}.d/"
  mkdir -p "$validation_folder"
  bsub "zcat -f $line | /nfs/production3/eva/software/vcf-validator/version-history/vcf_validator_0.7_summary -r summary,text,database -o $validation_folder"
done < ${main_folder}/files_to_validate.txt

mv ${main_folder}/files_to_validate.txt ${main_folder}/files_validated_in_last_cron_execution.txt
mv ${main_folder}/uploading.txt ${main_folder}/was_uploading_in_last_cron_execution.txt

touch ${main_folder}/last_cron_execution.txt

