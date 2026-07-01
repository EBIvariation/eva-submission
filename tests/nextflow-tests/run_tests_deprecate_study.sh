#!/bin/bash

set -Eeuo pipefail

# Clean up previous runs
rm -rf work

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}

# clusterOptions (and so the -o/-e logs it configures) are only honoured by grid executors,
# not by the local executor used when running these tests outside of CI
USING_SLURM=false
if grep -q "executor = 'slurm'" nextflow.config; then
    USING_SLURM=true
fi

log_dir=../../../project/logs
mkdir -p project/logs

# Create a minimal variant ID file and properties file for the test
echo "ss123456" > test_ssids.txt
touch drop_study.properties
touch deprecation.properties

# Create the CSV for the deprecation workflow
cat > source_deprecations.csv << 'EOF'
assembly_accession,variant_id_file,db_name
GCA_000001405.2,test_ssids.txt,eva_hsapiens_grch37
GCA_000001405.3,test_ssids.txt,eva_hsapiens_grch38
EOF


# Test 1: Run both deprecate_variants and drop_study
printf "\e[32m==== DEPRECATE STUDY - BOTH TASKS ====\e[0m\n"
nextflow run ${SOURCE_DIR}/deprecate_study.nf \
    -params-file test_deprecate_study_config.yaml \
    -c nextflow.config

printf "\e[32m====== Commands run (deprecate) ======\e[0m\n"
find work/ \( -name '*.out' -o -name '*.err' \) -exec cat {} \;

# Verify that both assemblies were processed (deprecation ran twice)
DEPRECATE_COUNT=$(grep -rl "deprecate.jar" work/ --include="*.command.sh" 2>/dev/null | wc -l)
printf "Deprecation processes run: ${DEPRECATE_COUNT}\n"
if [ "${DEPRECATE_COUNT}" -ne 2 ]; then
    echo "ERROR: Expected 2 deprecation processes, got ${DEPRECATE_COUNT}"
    exit 1
fi

# Verify that drop_study ran once per unique db_name (2 unique db_names here)
DROP_COUNT=$(grep -rl "drop-study-job" work/ --include="*.command.sh" 2>/dev/null | wc -l)
printf "Drop study processes run: ${DROP_COUNT}\n"
if [ "${DROP_COUNT}" -ne 2 ]; then
    echo "ERROR: Expected 2 drop_study processes (one per db_name), got ${DROP_COUNT}"
    exit 1
fi

# Verify that drop_study scripts reference the correct project accession
grep -r "input.study.id=PRJEB12345" work/ --include="*.command.sh" > /dev/null || {
    echo "ERROR: drop_study did not pass correct project accession"
    exit 1
}

if [ "${USING_SLURM}" = true ]; then
    # check that slurm actually submitted these jobs and wrote their -o/-e clusterOptions logs
    printf "\e[32m====== Slurm deprecate/drop_study logs ======\e[0m\n"
    ls project/logs/deprecate.test_ssids.txt_GCA_000001405.2.log project/logs/deprecate.test_ssids.txt_GCA_000001405.2.err
    ls project/logs/deprecate.test_ssids.txt_GCA_000001405.3.log project/logs/deprecate.test_ssids.txt_GCA_000001405.3.err
    ls project/logs/drop_study.eva_hsapiens_grch37_PRJEB12345.log project/logs/drop_study.eva_hsapiens_grch37_PRJEB12345.err
    ls project/logs/drop_study.eva_hsapiens_grch38_PRJEB12345.log project/logs/drop_study.eva_hsapiens_grch38_PRJEB12345.err
fi

# clean up
rm -rf work .nextflow*
rm -f test_ssids.txt drop_study.properties deprecation.properties source_deprecations.csv

# Test 2: Run deprecate_variants only
printf "\e[32m==== DEPRECATE STUDY - DEPRECATE VARIANTS ONLY ====\e[0m\n"
echo "ss123456" > test_ssids.txt
touch drop_study.properties
cat > source_deprecations.csv << 'EOF'
assembly_accession,variant_id_file,db_name
GCA_000001405.2,test_ssids.txt,None
EOF

nextflow run ${SOURCE_DIR}/deprecate_study.nf \
    -params-file test_deprecate_study_config.yaml \
    -c nextflow.config \
    --tasks deprecate_variants

DEPRECATE_COUNT=$(grep -rl "deprecate.jar" work/ --include="*.command.sh" 2>/dev/null | wc -l)
printf "Deprecation processes run: ${DEPRECATE_COUNT}\n"
if [ "${DEPRECATE_COUNT}" -ne 1 ]; then
    echo "ERROR: Expected 1 deprecation process, got ${DEPRECATE_COUNT}"
    exit 1
fi


# clean up
rm -rf work .nextflow*
rm -f test_ssids.txt drop_study.properties deprecation.properties source_deprecations.csv
rm -rf project

printf "\e[32m==== ALL DEPRECATE STUDY TESTS PASSED ====\e[0m\n"
cd ${cwd}
