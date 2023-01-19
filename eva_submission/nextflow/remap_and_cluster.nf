#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Remap a single study from one assembly version to another, cluster, and QC.

    Inputs:
            --taxonomy_id                   taxonomy id of submitted variants that needs to be remapped.
            --source_assemblies             assembly accessions this project's submitted variants are currently mapped to.
            --target_assembly_accession     assembly accession the submitted variants will be remapped to.
            --species_name                  scientific name to be used for the species.
            --genome_assembly_dir           path to the directory where the genome should be downloaded.
            --extraction_properties         path to extraction properties file
            --ingestion_properties          path to remapping ingestion properties file
            --clustering_properties         path to clustering properties file
            --clustering_instance           instance id to use for clustering
            --output_dir                    path to the directory where the output file should be copied.
            --remapping_config              path to the remapping configuration file
    """
}

params.source_assemblies = null
params.target_assembly_accession = null
params.species_name = null
params.memory = 8
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.taxonomy_id || !params.source_assemblies || !params.target_assembly_accession || !params.species_name || !params.genome_assembly_dir ) {
    if (!params.taxonomy_id) log.warn('Provide the taxonomy id of the source submitted variants using --taxonomy_id')
    if (!params.source_assemblies) log.warn('Provide source assemblies using --source_assemblies')
    if (!params.target_assembly_accession) log.warn('Provide the target assembly using --target_assembly_accession')
    if (!params.species_name) log.warn('Provide a species name using --species_name')
    if (!params.genome_assembly_dir) log.warn('Provide a path to where the assembly should be downloaded using --genome_assembly_dir')
    exit 1, helpMessage()
}

species_name = params.species_name.toLowerCase().replace(" ", "_")

// Create an channel that will either be empty if remapping will take place or contain a dummy value if not
// This will allow to trigger the clustering even if no remapping is required
// We're using params.genome_assembly_dir because cluster_studies_from_mongo needs to receive a file object
empty_ch = params.remapping_required ? Channel.empty() : Channel.of(params.genome_assembly_dir)

process retrieve_source_genome {
    when:
    source_assembly_accession != params.target_assembly_accession

    input:
    val source_assembly_accession from params.source_assemblies

    output:
    tuple val(source_assembly_accession), path("${source_assembly_accession}.fa"), path("${source_assembly_accession}_assembly_report.txt") into source_assembly

    """
    $params.executable.genome_downloader --assembly-accession ${source_assembly_accession} --species ${species_name} --output-directory ${params.genome_assembly_dir}
    ln -s ${params.genome_assembly_dir}/${species_name}/${source_assembly_accession}/${source_assembly_accession}.fa
    ln -s ${params.genome_assembly_dir}/${species_name}/${source_assembly_accession}/${source_assembly_accession}_assembly_report.txt
    """
}


process retrieve_target_genome {
    when:
    params.source_assemblies.any{it != params.target_assembly_accession}

    output:
    path "${params.target_assembly_accession}.fa" into target_fasta
    path "${params.target_assembly_accession}_assembly_report.txt" into target_report

    """
    $params.executable.genome_downloader --assembly-accession ${params.target_assembly_accession} --species ${species_name} --output-directory ${params.genome_assembly_dir}
    ln -s ${params.genome_assembly_dir}/${species_name}/${params.target_assembly_accession}/${params.target_assembly_accession}.fa
    ln -s ${params.genome_assembly_dir}/${species_name}/${params.target_assembly_accession}/${params.target_assembly_accession}_assembly_report.txt
    """
}

process update_source_genome {

    input:
    tuple val(source_assembly_accession), path(source_fasta), path(source_report) from source_assembly
    env REMAPPINGCONFIG from params.remapping_config

    output:
    tuple val(source_assembly_accession), path("${source_fasta.getBaseName()}_custom.fa"), path("${source_report.getBaseName()}_custom.txt") into updated_source_assembly

    """
    ${params.executable.custom_assembly} --assembly-accession ${params.source_assembly_accession} --fasta-file ${source_fasta} --report-file ${source_report}
    """
}

process update_target_genome {

    input:
    path target_fasta from target_fasta
    path target_report from target_report
    env REMAPPINGCONFIG from params.remapping_config

    output:
    path "${target_fasta.getBaseName()}_custom.fa" into updated_target_fasta
    path "${target_report.getBaseName()}_custom.txt" into updated_target_report

    """
    ${params.executable.custom_assembly} --assembly-accession ${params.target_assembly_accession} --fasta-file ${target_fasta} --report-file ${target_report} --no-rename
    """
}


/*
 * Extract the submitted variants to remap from the accessioning warehouse and store them in a VCF file.
 */
process extract_vcf_from_mongo {
    memory "${params.memory}GB"
    clusterOptions "-g /accession"

    when:
    source_assembly_accession != params.target_assembly_accession

    input:
    tuple val(source_assembly_accession), path(source_fasta), path(source_report) from updated_source_assembly

    output:
    // Only pass on the EVA vcf, dbSNP one will be empty
    tuple val(source_assembly_accession), path(source_fasta), path("${source_assembly_accession}_eva.vcf") into source_vcfs
    path "${source_assembly_accession}_vcf_extractor.log" into log_filename

    publishDir "$params.output_dir/logs", overwrite: true, mode: "copy", pattern: "*.log*"

    """
    java -Xmx8G -jar $params.jar.vcf_extractor \
        --spring.config.location=file:${params.extraction_properties} \
        --parameters.assemblyAccession=${source_assembly_accession} \
        --parameters.fasta=${source_fasta} \
        --parameters.assemblyReportUrl=file:${source_report} \
        > ${source_assembly_accession}_vcf_extractor.log
    """
}


/*
 * Variant remapping pipeline
 */
process remap_variants {
    memory "${params.memory}GB"

    input:
    tuple val(source_assembly_accession), path(source_fasta), path(source_vcf) from source_vcfs
    path target_fasta from updated_target_fasta

    output:
    tuple val(source_assembly_accession), path("${basename_source_vcf}_remapped.vcf") into remapped_vcfs
    path "${basename_source_vcf}_remapped_unmapped.vcf" into unmapped_vcfs
    path "${basename_source_vcf}_remapped_counts.yml" into remapped_ymls

    publishDir "$params.output_dir/eva", overwrite: true, mode: "copy", pattern: "*_eva_remapped*"

    script:
    basename_source_vcf = source_vcf.getBaseName()
    """
    # Setup the PATH so that the variant remapping pipeline can access its dependencies
    mkdir bin
    for P in $params.executable.bcftools $params.executable.samtools $params.executable.bedtools $params.executable.minimap2 $params.executable.bgzip $params.executable.tabix
      do ln -s \$P bin/
    done
    PATH=`pwd`/bin:\$PATH
    source $params.executable.python_activate
    # Nextflow needs the full path to the input parameters hence the pwd
    $params.executable.nextflow run $params.nextflow.remapping -resume \
      --oldgenome `pwd`/${source_fasta} \
      --newgenome `pwd`/${target_fasta} \
      --vcffile `pwd`/${source_vcf} \
      --outfile `pwd`/${basename_source_vcf}_remapped.vcf
    """
}


/*
 * Ingest the remapped submitted variants from a VCF file into the accessioning warehouse.
 */
process ingest_vcf_into_mongo {
    memory "${params.memory}GB"
    clusterOptions "-g /accession"

    input:
    tuple val(source_assembly_accession), path(remapped_vcf) from remapped_vcfs
    path target_report from updated_target_report

    output:
    path "${remapped_vcf}_ingestion.log" into ingestion_log_filename

    publishDir "$params.output_dir/logs", overwrite: true, mode: "copy", pattern: "*.log*"

    script:
    """
    java -Xmx8G -jar $params.jar.vcf_ingestion \
        --spring.config.location=file:${params.ingestion_properties} \
        --parameters.remappedFrom=${source_assembly_accession}
        --parameters.vcf=${remapped_vcf} \
        --parameters.assemblyReportUrl=file:${target_report} \
        > ${remapped_vcf}_ingestion.log
    """
}


/*
 * Cluster target assembly.
 */
process cluster_studies_from_mongo {
    memory "${params.memory}GB"
    clusterOptions "-g /accession"

    input:
    path ingestion_log from empty_ch.mix(ingestion_log_filename.collect())

    output:
    path "${params.target_assembly_accession}_clustering.log" into clustering_log_filename
    path "${params.target_assembly_accession}_rs_report.txt" optional true into rs_report_filename

    publishDir "$params.output_dir/logs", overwrite: true, mode: "copy", pattern: "*.log*"

    """
    java -Xmx8G -jar $params.jar.clustering \
        --spring.config.location=file:${params.clustering_properties} \
        --spring.batch.job.names=STUDY_CLUSTERING_JOB \
        > ${params.target_assembly_accession}_clustering.log
    """
}

/*
 * Run clustering QC job
 */
process qc_clustering {
    memory "${params.memory}GB"
    clusterOptions "-g /accession"

    input:
    path rs_report from rs_report_filename

    output:
    path "${params.target_assembly_accession}_clustering_qc.log" into clustering_qc_log_filename

    publishDir "$params.output_dir/logs", overwrite: true, mode: "copy", pattern: "*.log*"

    """
    java -Xmx8G -jar $params.jar.clustering \
        --spring.config.location=file:${params.clustering_properties} \
        --spring.batch.job.names=NEW_CLUSTERED_VARIANTS_QC_JOB \
        > ${params.target_assembly_accession}_clustering_qc.log
    """
}
