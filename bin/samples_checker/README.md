samples_checker module
======================

### What is This?
This simple tool suite is for checking if all the samples in the submitted files could be found in the submission xls.

### Prerequisites
```commandline
git clone https://github.com/EBIvariation/amp-t2d-submissions
cd amp-t2d-submissions/samples_checker
virtualenv -p python2.7 venv
source venv/bin/activate
pip install -r requirements.txt
deactivate
```

### Using the script
There is one script you can run under samples_checker subdirectory:
```commandline
check_samples.py
```
Before running these scripts, you'd better to activate virtualenv for correct environments:
```commandline
cd amp-t2d-submissions/samples_checker
source venv/bin/activate
```
Please refer to their command line help for information about what they do and which arguments are required. e.g.:
```commandline
python ./samples_checker/check_samples.py -h
```
Here are some of the examples you could try out:

For T2D:
```commandline
python ../xls2xml/xls2xml/xls2xml.py --conf tests/data/T2D_xls2xml_v2.conf --conf-key File --schema tests/data/T2D_xls2xml_v2.schema --xslt tests/data/T2D_xls2xml_v2.xslt tests/data/example_AMP_T2D_Submission_form_V2.xlsx tests/data/T2D_File.xml
python ../xls2xml/xls2xml/xls2xml.py --conf tests/data/T2D_xls2xml_v2.conf --conf-key Sample --schema tests/data/T2D_xls2xml_v2.schema --xslt tests/data/T2D_xls2xml_v2.xslt tests/data/example_AMP_T2D_Submission_form_V2.xlsx tests/data/T2D_Sample.xml
python ./samples_checker/check_samples.py --sample-xml tests/data/T2D_Sample.xml --file-xml tests/data/T2D_File.xml --file-path ./tests/data
```
For EVA:
```commandline
python samples_checker/check_samples_eva.py --metadata-file /nfs/production3/eva/submissions/ELOAD_402/10_submitted/submission_files/eva/Craniosynostotic_Rabbit_Colony_Metadata.xlsx --file-path /nfs/production3/eva/submissions/ELOAD_402/10_submitted/vcf_files
```
