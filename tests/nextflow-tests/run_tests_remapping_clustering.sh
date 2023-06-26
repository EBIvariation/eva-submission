#!/bin/bash

set -Eeuo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
SOURCE_DIR="$(dirname $(dirname $SCRIPT_DIR))/eva_submission/nextflow"

cwd=${PWD}
cd ${SCRIPT_DIR}
mkdir -p project project/accessions project/public ftp

printf "\e[32m==== REMAP AND CLUSTER PIPELINE ====\e[0m\n"
nextflow run ${SOURCE_DIR}/remap_and_cluster.nf -params-file test_ingestion_config.yaml \
   --taxonomy_id 1234 \
	 --species_name "Thingy thungus" \
	 --genome_assembly_dir ${SCRIPT_DIR}/genomes \
	 --extraction_properties ${SCRIPT_DIR}/template.properties \
	 --ingestion_properties ${SCRIPT_DIR}/template.properties \
	 --clustering_properties ${SCRIPT_DIR}/template.properties \
	 --clustering_instance 1 \
	 --output_dir ${SCRIPT_DIR}/output \
	 --logs_dir ${SCRIPT_DIR}/output/logs \
	 --remapping_config ${SCRIPT_DIR}/test_ingestion_config.yaml \
	 --memory 2

# Two remappings
printf "\e[32m====== Remapping files ======\e[0m\n"
ls ${SCRIPT_DIR}/output/eva/GCA_0000001_eva_remapped.vcf \
   ${SCRIPT_DIR}/output/eva/GCA_0000001_eva_remapped_unmapped.vcf \
   ${SCRIPT_DIR}/output/eva/GCA_0000001_eva_remapped_counts.yml \
   ${SCRIPT_DIR}/output/eva/GCA_0000002_eva_remapped.vcf \
   ${SCRIPT_DIR}/output/eva/GCA_0000002_eva_remapped_unmapped.vcf \
   ${SCRIPT_DIR}/output/eva/GCA_0000002_eva_remapped_counts.yml

# Test we have 8 log files in the logs directory (2 extraction, 2 ingestion, 1 clustering 1 clustering qc
# and 2 backpropagations)
printf "\e[32m====== Remapping ingestion and clustering logs ======\e[0m\n"
ls ${SCRIPT_DIR}/output/logs/GCA_0000001_vcf_extractor.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000001_eva_remapped.vcf_ingestion.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000002_vcf_extractor.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000002_eva_remapped.vcf_ingestion.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000003_clustering.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000003_clustering_qc.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000003_backpropagate_to_GCA_0000001.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000003_backpropagate_to_GCA_0000002.log \
   ${SCRIPT_DIR}/output/logs/GCA_0000003_rs_report.txt


# clean up
rm -rf work .nextflow*
rm -r output genomes
cd ${cwd}
