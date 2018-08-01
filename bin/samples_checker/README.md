Verify study samples in VCFs against those in the EVA study metadata sheet
==========================================================================

### What is This?
This is a simple tool to check if all the samples in the submitted files for a study could be found in the Submission Metadata sheet for that study and vice-versa.

### Prerequisites
```bash
git clone https://github.com/EBIvariation/eva-submission
cd eva-submission/bin/samples_checker
virtualenv -p python2.7 venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

cd ../..
git clone https://github.com/EBIvariation/amp-t2d-submissions
cd amp-t2d-submissions/samples_checker
virtualenv -p python2.7 venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

cd ../xls2xml
virtualenv -p python2.7 venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

### Using the script
Before running the script, activate virtualenv:
```bash
cd eva-submission/bin/samples_checker
source venv/bin/activate
```

After activating the virtualenv as described above, you can run the script as follows:
```bash
python samples_checker/check_samples_eva.py --metadata-file /nfs/production3/eva/submissions/ELOAD_402/10_submitted/submission_files/eva/Craniosynostotic_Rabbit_Colony_Metadata.xlsx --vcf-files-path /nfs/production3/eva/submissions/ELOAD_402/10_submitted/vcf_files
```
