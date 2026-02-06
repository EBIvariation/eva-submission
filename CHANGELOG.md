Changelog for eva-submission
============================

## 1.19.14 (unreleased)
-----------------------

- Nothing changed yet.


## 1.19.13 (2026-02-06)
-----------------------

- Add script to support deprecation of submitted variants
- Search/Infer for project level taxonomy if not provided in the metadata
- Add QC process to detect duplicate SS
- Publish the clustering duplicate detection log
- Bump pyutils version (updates to Spring properties)


## 1.19.12 (2026-01-15)
-----------------------

## Ingestion 
 - Load single sequence accession in EVAPRO to support single sequence with archive only
 - Force accessioning to use PREFILTER_EXISTING to speed up accession of variant already existing
 - Update prepare_backlog to support cases when no metadata has been ingested


## 1.19.11 (2026-01-07)
-----------------------


## Ingestion 
 - Support for NCBI project during metadata ingestion
 - Use correct job name parameter in accessioning

## 1.19.10 (2025-12-19)
-----------------------

## Brokering
- Use single value for PROGRAM when brokering to ENA, and add individual values as analysis attributes

## Ingestion
- Handle ENA errors when getting database name and target assembly


## 1.19.9 (2025-12-18)
----------------------

## Brokering
- Use XML to broker to ENA

## Ingestion
- Retrieve experiment type from the ELOAD when not available from ENA.
- Add EVA submission even if no ENA submission exists

## Other
- Fix sample modification/curation script 

## 1.19.8 (2025-12-12)
----------------------

- Brokering fixes
- Change modify_existing_sample to accept a spreadsheet or a json file


## 1.19.7 (2025-12-08)
----------------------

### Validation
- Print validation reports after completion

### Brokering 
- Option to use the ENA XML format instead of the JSON format
- Fix sample name retrieval when they already exist in BioSamples


## 1.19.6 (2025-11-13)
----------------------

### Validation
 - Integrate eva-sub-cli tasks for more flexibility

### Brokering
 - Update the reference with the one used in the validation before brokering 
 - Add default for collection date and geographic location when missing 

Other small fixes from prod

## 1.19.5 (2025-11-04)
----------------------

### Preparation 
 - Compare file names using their base names between metadata and file system

### Brokering
 - Support adding analysis to existing project

### Deletion
 - Archive the content of the ELOAD directory with the ELOAD directory at the top


## 1.19.4 (2025-10-22)
----------------------

### All
- Add new command line parameter to pass nextflow configuration 

### Deletion
 - Add multiple fixes for deletion of incomplete ELOAD

## 1.19.3 (2025-10-10)
----------------------

### Deletion
 - Support for deletion of ELOAD even if not completed


## 1.19.2 (2025-10-09)
----------------------

### Brokering
 - Keep track of Sample names when they differ in the VCF and the BioSample

### Ingestion
 - New simple archiving that only loads the metadata and save files to the FTP


## 1.19.1 (2025-09-24)
----------------------

### Validation
 - Convert spreadsheet to json only if it is present
 - Remove the option to merge for eva-submission
 - Copy entire validation_output directory
 - Fix how aggregation type saved to config

### Brokering
 - Parse the ENA receipt based on the content of the response
 - Fix the software list provided to ENA
 - Convert Biosample with the wrong properties

### Ingestion
 - Use webin v1 when getting the hold date for a receipt
 - Fix Ingestion of metadata from ENA


## 1.19.0 (2025-09-04)
----------------------

### Validation: 
 - Use eva-sub-cli to validate the submission instead of individual tests
 - Validation through individual task is deprecated

### Brokering:
 - Use the metadata JSON to broker to BioSamples and ENA
 - The brokering through the XML is deprecated

### Ingestion: 
 - Ensure the EVAPRO refresh of the study browser is committed

### Other:
 - Old ELOAD samples resolution is improved
 - Specify the branch from which the integration test will be triggered 

## 1.18.6 (2025-07-17)
----------------------

- Convert from both INSDC and RefSeq contig accessions
- Retrieve the taxonomy from the config when not in the ENA project
- Remove files_from_ena
- EVA-3855: Fix sample check when sampleInVCF != bioSampleName
- Add trigger for integration tests
- Wrap literal query with text() function
- EVA-3854 - Make nextflow task fail when one of the command fails
- Ensure failure when no archive folder is found


## 1.18.5 (2025-07-07)
----------------------

- Change property brokered to BioSample from "collection_date" to "collection date" 
- Allow multiple date format during xlsx conversion 


## 1.18.4 (2025-06-10)
----------------------

- prepare_submission does not need the submission account to run
- Use basename when overwriting path in metadata

## 1.18.3 (2025-05-27)
----------------------

- Run eva-sub-cli as part of validation


## 1.18.2 (2025-05-22)
----------------------

 - Validation: Better conversion of the new spreadsheet format
 - New script to load samples from previous submissions


## 1.18.1 (2025-05-14)
----------------------

### Ingestion

- Metadata load fixes: 
  - allow multiple parent project
  - remove unique constraint on project_eva_submission
  - auto increment project.eva_study_accession manually
- Pass assembly report to eva-pipeline to trigger contig renaming to INSDC
- Fix and Refactor ingestion QC and make deletion dependent on successful QC 

## 1.18.0 (2025-05-08)
----------------------

- Replace the perl script to perform metadata ingestion with python script
- Ingest sample metadata information from BioSample and ENA 
- Small fix for qc_submission to handle skip statistic check
- small fix for detection of basic aggregation 

## 1.17.1 (2025-04-14)
----------------------

- Convert new spreadsheet template back to 1.1.4
- Improved resolution of GCA
- Remove connection to AAP authentication


## 1.17.0 (2025-02-26)
----------------------
- Do not need to count star * allele when checking accessioning QC
- Validate sample check when VCF contains aggregated genotypes
- Initial processing code for eva-sub-cli
- Automation for new clustering QC job
- Check for duplicate analysis alias during validation
- Sort after normalisation
- Various other bugfixes

## 1.16.1 (2024-10-25)
----------------------

## Validation
 - New process to correct BioSamples imported from NCBI

## 1.16 (2024-09-11)
----------------------

## All
 - Move the data/project directories to the eload directory

## Validation
 - Detect contig naming convention in VCF file 
 - Fallback on manual VCF file parsing when running sample check as pysam fails sometime 
 - Fix validation output file renaming in SV and naming convention 
 - Fix aggregation check 

## Brokering 
 - Fix: ENA platform and Imputation attributes
 - Actually keep the samples that were created

## Ingestion 
 - Fix import of metadata for analysis only  
 - Fix Async upload to ENA
 - Run new statistic calculation step 
 - Install Vep version if not available 

## 1.15.8 (2024-07-19)
----------------------

## Validation
 - Fallback on manual VCF file parsing when running sample check as pysam fails sometime 

## Brokering 
 - Allow derive sample from multiple sample accessions 

## QC 
 - Check that VEP has been run  before QC

## Other
 - Refactor biosamples communicators into pyutils 
 - Initial version of an orchestrator for submission processing 


## 1.15.7 (2024-05-15)
----------------------

Add label to nextflow process to better support SLURM

## ingestion
- Remove instance id for clustering and accessioning
- Slight improvement in VEP cache retrieval 

## 1.15.6 (2024-04-23)
----------------------

- Use NCBI eutils key whenever possible.
- New script to test sample ownership.


## 1.15.5 (2024-04-12)
----------------------

Allow insert_new_assembly.py to insert a new taxonomy only

## ingestion
- Wait for all the accessioning to be complete before ingesting accessioning report in the variant warehouse


## 1.15.4 (2024-03-13)
----------------------

### Validation 
 - Remove platform restriction and add experiment type value

### Brokering 
 - confirm samples brokering using existing and novel sample names

## ingestion QC 
 - Skip check for VEP if not run
 - Fix remapping ingestion log file name

## 1.15.3 (2024-02-09)
----------------------

- qc_submission support optional statistic calculation
- Improve collection date and geographic location check for pre-submitted samples 

## 1.15.2 (2024-01-29)
----------------------

- Ensure annotation & statistics calculation are run per analysis
- Update qc_submission to check the new split variant load logs

## 1.15.1 (2024-01-16)
----------------------

- Fix missing module


## 1.15 (2024-01-15)
----------------------

### Preparation
 - Create a dummy assembly report when none exists for sequences 

### Validation
 - Validate existing BioSamples

### Brokering
 - Allow the Link label to be the link if no label is provided
 - New script to update Biosamples
 - Pass single sequence accession to ENA

### Ingestion
 - Split variant load
 - During variant remapping extract VCF file with taxonomy id
 - Normalise VCF before accessioning/variant load
 - Merge accessioning and variant load in a single nextflow workflow 


## 1.14.1 (2023-11-02)
--------------------

### Brokering
 - Fix submission to existing project accession (see [PR177](https://github.com/EBIvariation/eva-submission/pull/177))
 - New script to Insert a publication in EVAPRO (see [PR180](https://github.com/EBIvariation/eva-submission/pull/180))

### Ingestion 
 - Change the default ingestion instance to 2 (see [PR182](https://github.com/EBIvariation/eva-submission/pull/182))

## 1.14 (2023-10-30)
--------------------

- Use new version of eva-common-pyutils (v0.6.2).
- Refactor to use new version of pyutils. See [here](https://github.com/EBIvariation/eva-submission/pull/174).

### Validation
- Add check for ENA projects and Biosamples accession. See [here](https://github.com/EBIvariation/eva-submission/pull/173).
- Improve novel attributes validation. See [here](https://github.com/EBIvariation/eva-submission/pull/176).
- Do not validate biosamples when we update sample info. See [here](https://github.com/EBIvariation/eva-submission/pull/178).

### Brokering
- Do not catch errors during normalisation. See [here](https://github.com/EBIvariation/eva-submission/pull/172).

### Ingestion
- Use Ensembl rapid release and project assembly as fallbacks for target assembly. See [here](https://github.com/EBIvariation/eva-submission/pull/170).


## 1.13 (2023-09-21)
--------------------

### Validation
- Fix unique analysis usage in Prepare submission and ENA brokering
- Flag for normalisation to warn on reference check issues rather than fail
- Fix validation of date in metadata
- Update the reference assembly in the metadata spreadsheet before brokering

### Brokering
- Fix link retrieval from upload single file to ENA
- Hack to remove the null values in external reference

## 1.12 (2023-07-17)
--------------------

### Test
 - Add missing metadata files and process

### Validation
 - Make project and analysis alias unique by prepending the ELOAD number

### Brokering 
 - Remove spaces between novel attributes
 - Improve formatting of archival text
 - Brokering results overwrite previous one.

### Ingestion
 - Disable check for presence of extra file in the 30_valid dir
 - Check that the project exists in EVAPRO before loading from ENA only load the analysis if it does
 - Fix to QC stating that FTP files are missing


## 1.11 (2023-07-03)
----------------------

### Validation
 - Remove normalisation

### Brokering 
 - Run normalisation in prepare brokering 

### Ingestion
 - Move log directory for remapping and clustering from 53_clustering to 00_logs

## 1.10.8 (2023-06-22)
----------------------

- New script to retrieve archived ELOAD from LTS
- Fix bug qc_submission.py


1.10.7 (2023-06-20)
------------------
### Test
 - Add docker image to represent eva-submission run environment for testing

### Validation
 - Enforce the presence of sample collection data and geographic location

### Brokering 
 - Add default geolocation and collection dates if missing

1.10.6 (2023-06-05)
------------------
### Java pipeline processes
 - accept multiple mongos hosts in connection strings (see EVA-3253)

1.10.5 (2023-05-18)
------------------
### Ingestion
 - fix accession import log file name (see EVA-3243)

1.10.4 (2023-05-09)
------------------
### Ingestion
 - Use Spring properties generator to generate application properties for the various Java pipelines (see EVA-3147)
 - Determine remapping target assembly for a submission (see EVA-3208)

1.10.3 (2023-04-04)
------------------
### Ingestion
 - Fix nextflow process for creation of properties file

1.10.2 (2023-04-03)
------------------
### Preparation
 - Fix how the species name in Ensembl is resolved within the VEP cache 

1.10.1 (2023-03-24)
------------------
### Ingestion
 - default clustering instance to 6
 - Bugfixes for accession load  

1.10.0 (2023-03-23)
------------------

### Validation
- Prepare reference genome to allow normlaisation to happen without error 

### Brokering
- Add population and imputation to BioSamples when available

### Ingestion
- Load Submitted variant accession to the variant warehouse during variant load
- Add remapping, clustering and backpropagation steps
- skip remapping, clustering and backpropagation when target assembly is from a different taxonomy

1.9.0 (2023-01-25)
------------------

### Validation
- Separate structural variation check in the Nextflow

### Brokering
- Generate archival confirmation text after brokering

### Ingestion
- Fix to QC report

1.8.0 (2022-10-13)
------------------

### Validation
- Add normalisation step during validation. The file used after that will be the normalised file.

### Brokering 
- Fix brokering to existing project (use ELOAD as project alias)

### Ingestion
- New validation script that checks verify all ingestion steps have been successful and the data appear where it should.

### Other
- New script to add a taxonomy/assembly to the metadata ()


1.7.0 (2022-09-09)
------------------

### Validation
- Add new validation that normalise all the input VCF files as they are being validated
- Make validations run through nextflow run based on the tasks specified on command line
- 

### Backlog preparation
- Create CSI index files


1.7.0 (2022-09-09)
------------------

### Validation
- Detect VCFs with structural variants

### Backlog preparation
- Create CSI index files

1.6.3 (2022-08-02)
------------------

### Brokering
 - Upload FTP to ENA FTP is resumable and retryable. 
 - New option for dryrun for ENA upload 
 - Fix metadata read:
   - Support for Analysis list in Sample Sheet
   - Serialise dates when uploading to BioSamples

1.6.2 (2022-07-05)
------------------

### Brokering
 - Hot fix for update to BioSample during brokering 

1.6.1 (2022-07-04)
------------------

### Preparation
 - Update the Contig alias while downloading a new genome

### Brokering
 - Use the curation object to update any BioSamples
 - Add EVA Study link URL to all BioSamples created


1.6.0 (2022-05-16)
------------------

 - Write the configuration file explicitly via the context manager

### Brokering
 - Make BioSamples brokering more robust
 - Allow submission to ENA via asynchronous endpoint

1.5.2 (2022-05-03)
------------------

### Brokering
 - csi generated without .gz to accomodate ENA validation

1.5.1 (2022-04-29)
------------------

### Validation
 - Check assembly report for multiple synonyms for the same contig

### Brokering
 - Remove tbi index generation and upload to ENA

### Ingestion
 - Support for metadata load when adding to an existing project

### Misc
 - migrate script do not crash when specifying a project that has not been used yet

1.5.0 (2022-03-28)
------------------

### Ingestion 
 - Fix VEP cache download and directory extraction  

### Backlog preparation
 - New option to keep the configuration as is
 - update_metadata script now check assembly_set_id coherence
 - New script to retrieve VCF and tabix files from ENA when they do not exist in EVAPRO

### Codon support 
 - automated deployment in codon
 - Add support for FTP copies in datamovers nodes 
 - New Script to migrate in-progress submissions
 - Set a cipher that will work on Codon

1.4.3 (2022-02-11)
------------------

### Backlog preparation
 - New option to force validation to pass
 - Enable project and analysis accessions to be set on command line

### Ingestion
 - Ability to resume Nextflow processes within ingestion
 
1.4.2 (2022-02-04)
------------------

### Backlog preparation
 - Require latest vcf_merge and retrieve the csi index if it exists

### Ingestion
 - Fix for running annotation only

1.4.1 (2022-01-26)
------------------

### Accessioning
 - Adding count service credentials to accessioning properties file 

1.4.0 (2022-01-21)
------------------

### All
 - Adding logging to single file across each step  

### Brokering
 - Update metadata file correctly after merge

### Backlog preparation
 - Fix bug in post merge check 

### Ingestion
 - Allow running loading step with annotation only 
 - Add warning when the browsable files are different from analysis files 

1.3.2 (2021-11-29)
------------------
 
### Brokering:
 - Do not retry brokering preparation when it has already been done
 - Fix brokering to existing projects
 
### Backlog preparation
 - Share ELOAD config to ensure prep and validation can communicate

1.3.1 (2021-10-28)
------------------

### Ingestion:
 - Fix the annotation metadata collection name

1.3 (2021-10-14)
------------------
 
### Ingestion:
 - metadata: update assembly_set_id in analysis after loading from ENA
 - VEP cache resolution is based on the assembly

### Backlog preparation
 - New option to merge the VCF before ingestion

### Other
 - new script to run metadata update if they did not occur during ingestion

1.2.0 (2021-10-07)
------------------

### Validation:
 - Check the aggregation type of VCF files during the validation
 
### Ingestion:
 - Use aggregation type detected during validation
 - Resolve and Create (if required) the variant warehouse databases
 - Resolve and download the correct VEP cache or skip annotation  

1.1.1 (2021-09-22)
------------------
 - Fix error strategy in Nextflow

1.1.0 (2021-09-21)
-------------------

### Validation:
 - Detect merge can be performed
 - Check analysis alias collision
 - New option to merge file post validation

### Brokering:
 - Add analysis to existing project

### Ingestion:
 - resolve symlinks during variant load to avoid confusing eva-pipeline
 - Update metadata after variant load


1.0.3 (2021-07-21)
-------------------

 - Fixes to variant load pipeline and ENA queries.

1.0.2 (2021-07-19)
-------------------

 - Additional bug fixes from 1.0

1.0.1 (2021-07-15)
-------------------

 - Bug fixes from 1.0 release.

1.0 (2021-07-15)
-----------------

### General
 - Support for multiple analyses and different reference sequence per analysis
 - Version number now stored in eload config, backwards incompatible configs will be upgraded

### Backlog + Ingestion:
 - Bug fix for getting hold dates from ENA
 - Refactor database connection methods

### Ingestion:
 - Skip VEP annotation based on command-line parameter
 - Get VEP versions from database when possible

v0.5.9 (2021-07-06)
-------------------

 - Fix bug with hold date checking metadata db before loaded from ENA.

0.5.8 (2021-06-16)
-------------------

### Validation
 - Speedup of excel reader

### Ingestion
 - Retrieve hold date from ENA rather than config
 - Use secondaryPreferred reads in Mongo 4.0 environment

### Backlog preparation
 - Add ability to force replacing the config (previous one is backed up)
 - Retrieve the reference accession from the analysis in metadata database
 - Retrieve files from ENA when not available locally
 - Support for multiple files per analysis

0.5.7 (2021-06-01)
-------------------
 - Fix missing Nextflow and etc directory

0.5.6 (2021-05-28)
-------------------
 - Make scripts in bin executable
 - Various bugfixes in genome downloader, validation reporting, backlog preparation, and ingestion
