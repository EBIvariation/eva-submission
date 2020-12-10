# Submission automation


These scripts are meant to be used by EVA helpdesk personnel to manage submissions to the EVA.
They process the submission in a semi-automated way and record the outcome of each step in a yaml file located in a given submission directory


## Scripts


### Information about a submission

The following script simply surveys the content of FTP boxes and returns a short text that should provide enough information to create the submission ticket.

```bash
python detect_submission.py --ftp_box 3 --username john
```

### Copy relevant file to the submission folder

This script can be used to grab the relevant files from the ftp_box and to copy them to the submission folder when you supply `--ftp_box` and `--username` parameters. 
It will also prepare the submission folder and create the directory structure  and the config file required for the rest of the execution.
Without the above parameters, it only prepares the submission folder.


```bash
python prepare_submission.py --ftp_box 3 --username john --eload 677
```

or 

```bash
python prepare_submission.py --eload 677
```

### Validate the submitted data

This script will run the validation required to ensure that the data is suitable to be archived. It will check
 - The sample names in the VCF and metadata matches
 - The VCF conforms to the [submission standards](https://www.ebi.ac.uk/eva/?Help#submissionPanel)
 - The VCF reference allele match the reference genome.
 
 ```bash
python validate_submission.py --eload 677
```
