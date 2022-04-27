#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Prepare vcf file ready to be broker to ENA.

    Inputs:
            --vcf_files          list of vcf files that are meant to be prepared
            --output_dir         output_directory where the final will be written

    """
}

params.vcf_files = null
// executables
params.executable = ["md5sum", "tabix", "bgzip", "bcftools"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files) {
    if (!params.vcf_files)    log.warn('Provide a input vcf file using --vcf_files')
    if (!params.output_dir)    log.warn('Provide an output directory where the output file will be copied using --output_dir')

    exit 1, helpMessage()
}

// vcf files are used multiple times
vcf_channel = Channel.fromPath(params.vcf_files)

/*
* compress the VCF file
*/

process compress_vcf {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    path vcf_file from vcf_channel

    output:
    path "output/*.gz" into compressed_vcf1
    path "output/*.gz" into compressed_vcf2

    """
    mkdir output
    if [[ $vcf_file =~ \\.gz\$ ]]
    then
        gunzip -c $vcf_file | $params.executable.bgzip -c > output/$vcf_file
    else
        $params.executable.bgzip -c $vcf_file > output/${vcf_file}.gz
    fi
    """
}

/*
* Index the compressed VCF file
*/

process csi_index_vcf {

    publishDir "$params.output_dir",
            overwrite: true,
            mode: "copy"

    input:
    path compressed_vcf from compressed_vcf1

    output:
    path "${compressed_vcf}.csi" into csi_indexed_vcf

    """
    $params.executable.bcftools index -c $compressed_vcf
    """
}



/*
* md5 the compressed vcf and its index
*/

process md5_vcf_and_index {

    publishDir "$params.output_dir",
            overwrite: true,
            mode: "copy"

    input:
    path vcf from compressed_vcf
    path index from csi_indexed_vcf

    output:
    path "${vcf}.md5" into vcf_md5
    path "${index}.md5" into index_md5

    """
    $params.executable.md5sum ${vcf} > ${vcf}.md5
    $params.executable.md5sum ${index} > ${index}.md5
    """
}


