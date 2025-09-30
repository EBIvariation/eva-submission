#!/bin/bash

set -Eeuo pipefail

echo "run_tests_validation.sh"
run_tests_validation.sh

echo "run_tests_prep_brokering.sh"
run_tests_prep_brokering.sh

echo "run_tests_simple_archive.sh"
run_tests_simple_archive.sh

echo "run_tests_accession_and_variant_load.sh"
run_tests_accession_and_variant_load.sh

echo "run_tests_accession_and_variant_load_human.sh"
run_tests_accession_and_variant_load_human.sh

echo "run_tests_clustering.sh"
run_tests_clustering.sh

echo "run_tests_remapping_clustering.sh"
run_tests_remapping_clustering.sh
