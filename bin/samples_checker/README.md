Verify samples in VCF file match those in the study metadata sheet
==================================================================

This is a simple tool to check if all the samples in the submitted files for a study could be found in the Submission Metadata sheet for that study and vice-versa.

### Dependencies
```bash
# Clone this repo and install its Python dependencies
git clone https://github.com/EBIvariation/eva-submission
cd eva-submission/bin/samples_checker
virtualenv -p python2.7 venv
source venv/bin/activate
pip install -r requirements.txt
deactivate

# Get samples_checker and xls2xml modules from Github and install their Python dependencies
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

### Run
Before running the script, activate virtualenv:
```bash
cd eva-submission/bin/samples_checker
source venv/bin/activate
```

After activating the virtualenv as described above, you can run the script as follows:
```bash
python check_samples_eva.py --samples-checker-dir [path to the directory with the samples_checker module] --xls2xml-dir [path to the directory with the xls2xml module] --metadata-file [path to study Metadata XLS or XLSX file] --vcf-files-path [path to VCF files for the study]
```
