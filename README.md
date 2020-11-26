#Submission automation


These scripts are meant to be use by EVA helpdesk personel to manage submissions to EVA.
They process the submission in a semi-automated way and record the outcome of each step in a yaml file siting in each submission directory


## Scripts


### Information about a submission

This simply survey the content of FTP boxes and return a short text that should provide enough information to create the submission ticket.

```bash
python detect_submission.py --ftp_box 3 --username john
```

### Copy relevant file to the submission folder

This script can be used to grab the relevant files from the ftp_box and to copy them to the submission folder when you supply `--ftp_box` and `--username` parameters
It will also prepare the submission folder and create the directory structure  and the config file required for the rest of the execution.
without the ftp parameters it only prepare the submission folder.


```bash
python prepare_submission.py --ftp_box 3 --username john --eload 677
```

or 

```bash
python prepare_submission.py --eload 677
```

### Validate the submitted data

This script will run the validation required to ensure that the data id suitable to be archived. It will check
 - The sample names in the VCF and metadata matches
 - The VCF is conform to the check
 - The VCF reference allele match the reference genome.
 
 ```bash
python validate_submission.py --eload 677
```
