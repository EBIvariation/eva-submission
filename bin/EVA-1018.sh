
for i in `find /nfs/ftp/private/eva-box-* -name "*vcf.gz" -mmin -1440`;
do bsub "zcat $i | /nfs/production3/eva/software/vcf-validator/version-history/vcf_validator_0.7_summary -r summary,text,database -o /nfs/production3/eva/submissions/new_files_validations/"; done
sleep 1m
find /nfs/production3/eva/submissions/new_files_validations/ -name "*" -mmin -1400 > /nfs/production3/eva/submissions/new_files_validations/newfiles.txt 
if [[ $(find /nfs/production3/eva/submissions/new_files_validations/newfiles.txt -mmin -1400) ]];
then
mutt -s "New VCFs for validation in FTP dropbox area" garys@ebi.ac.uk < /nfs/production3/eva/submissions/new_files_validations/newfiles.txt
fi

