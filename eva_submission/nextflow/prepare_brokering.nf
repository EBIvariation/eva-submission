#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Prepare vcf file ready to be broker to ENA.

    Inputs:
            --input_vcfs         csv file containing the input vcf files and their respective reference fasta
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
if (!params.input_vcfs) {
    if (!params.input_vcfs)    log.warn('Provide a csv containing the input vcf files and the references using --input_vcfs')
    if (!params.output_dir)    log.warn('Provide an output directory where the output file will be copied using --output_dir')
    exit 1, helpMessage()
}

Channel.fromPath(params.input_vcfs)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf_file), file(row.fasta))}
        .set{input_vcfs_ch}

/*
* Normalise the VCF files
*/
process normalise_vcf {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    set file(vcf_file), file(fasta) from input_vcfs_ch

    output:
    path "output/*.gz" into compressed_vcf1
    path "output/*.gz" into compressed_vcf2

    """
    mkdir output
    if [[ $vcf_file =~ \\.gz\$ ]]
    then
        $params.executable.bcftools norm -f $fasta -O z -o output/$vcf_file $vcf_file
    else
        $params.executable.bcftools norm -f $fasta -O z -o output/${vcf_file}.gz $vcf_file output/${vcf_file}.gz
    fi
    """
}

/*
* Index the normalised compressed VCF file
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
    path vcf from compressed_vcf2
    path index from csi_indexed_vcf

    output:
    path "${vcf}.md5" into vcf_md5
    path "${index}.md5" into index_md5

    """
    $params.executable.md5sum ${vcf} > ${vcf}.md5
    $params.executable.md5sum ${index} > ${index}.md5
    """
}


