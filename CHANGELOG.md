Changelog for eva-submission
============================


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
