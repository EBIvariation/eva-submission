#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Validate a set of VCF files and metadata to check if they are valid to be submitted to EVA.

    Inputs:
            --vcffiles            list of vcf files that are meant to be validated
            --metadata_file       spreadsheet meant to be validated
            --reference_accession source genome file used to discover variants from the file provided with --vcffile [required]
    """
}

params.vcffiles = null
params.metadata_file = null
params.reference_accession = null
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcffiles || !params.metadata_file || !params.reference_accession) {
    if (!params.vcffiles)    log.warn('Provide a input vcf file using --vcffiles')
    if (!params.metadata_file)  log.warn('Provide a spreadsheet with --metadata_file')
    if (!params.reference_accession)    log.warn('Provide a accession for the genome using --reference_accession')
    exit 1, helpMessage()
}


vcf_channel = Channel.fromPath(params.vcffiles)
/*
* Validate the VCF file format
*/

process check_vcf_format {

    input:
        path vcf_file from vcf_channel

    output:
        path "vcf_validation*"

    validExitStatus 0,1

    """
    mkdir -p vcf_validation
    vcf_validator -i $vcf_file  -r database,text -o vcf_validation
    """
}




/*
* Validate the VCF reference allele
*/

process download_reference_genome {

    input:
        val accession from params.reference_accession
        val species from params.species

    output:
        path "output/*/$accession/$accession.fa" to reference_fasta
        path "output/*/$accession/$accession_assembly_report.txt" to reference_report

    """
    mkdir -p output
    genome_downloader -a $accession -s $species -o output
    """
}


/*
* Validate the VCF reference allele
*/

process check_vcf_reference {

    input:
        path vcf_file from vcf_channel
        path "reference_fasta" from reference_fasta
        path "reference_report" from reference_report

    output:
        path "assembly_check*"

    """
    mkdir -p vcf_validation
    vcf_assembly_checker -i  -f reference_fasta -a reference_report -r summary,text,valid  -o assembly_check
    """
}

