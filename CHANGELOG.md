
Changelog for ebi_eva_common_pyutils
===========================

1.2.0
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
