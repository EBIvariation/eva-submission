#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Validate a set of VCF files and metadata to check if they are valid to be submitted to EVA.

    Inputs:
            --vcf_files           list of vcf files that are meant to be validated
            --metadata_file      spreadsheet meant to be validated
            --reference_fasta    input fasta file used to verify the reference allele provided with --reference_fasta [required]
            --reference_report   input report providing the known chromosome name aliases provided with --reference_report [required]
            --output_dir         output_directory where the reports will be ouptut
    """
}

params.vcf_files = null
params.metadata_file = null
params.reference_fasta = null
params.reference_report = null
params.output_dir = null
// executables
params.executable =["vcf_assembly_checker": "vcf_assembly_checker", "vcf_validator": "vcf_validator"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files || !params.metadata_file || !params.reference_fasta || !params.reference_report || !params.output_dir) {
    if (!params.vcf_files)    log.warn('Provide a input vcf file using --vcf_files')
    if (!params.metadata_file)  log.warn('Provide a spreadsheet with --metadata_file')
    if (!params.reference_fasta)    log.warn('Provide a fasta file for the genome using --reference_fasta')
    if (!params.reference_report)    log.warn('Provide a assembly report file for the genome using --reference_report')
    if (!params.output_dir)    log.warn('Provide an output directory where the reports will be copied using --output_dir')
    exit 1, helpMessage()
}

// vcf files are used multiple times
vcf_channel1 = Channel.fromPath(params.vcf_files)
vcf_channel2 = Channel.fromPath(params.vcf_files)

/*
* Validate the VCF file format
*/

process check_vcf_valid {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
        path vcf_file from vcf_channel1

    output:
        path "vcf_format/*.vcf.errors.*.db" into vcf_validation_db
        path "vcf_format/*.vcf.errors.*.txt" into vcf_validation_txt
        path "vcf_format/*.vcf_format.log" into vcf_validation_log

    validExitStatus 0,1

    """
    mkdir -p vcf_format
    $params.executable.vcf_validator -i $vcf_file  -r database,text -o vcf_format > vcf_format/${vcf_file}.vcf_format.log 2>&1
    """
}


/*
* Validate the VCF reference allele
*/

process check_vcf_reference {

    publishDir "$params.output_dir",
            overwrite: true,
            mode: "copy"

    input:
        path "reference.fa" from params.reference_fasta
        path "reference.report" from params.reference_report
        path vcf_file from vcf_channel2

    output:
        path "assembly_check/*valid_assembly_report*" into vcf_assembly_valid
        path "assembly_check/*text_assembly_report*" into assembly_check_report
        path "assembly_check/*.assembly_check.log" into assembly_check_log

    validExitStatus 0,1

    """
    mkdir -p assembly_check
    $params.executable.vcf_assembly_checker -i $vcf_file -f reference.fa -a reference.report -r summary,text,valid  -o assembly_check > assembly_check/${vcf_file}.assembly_check.log 2>&1
    """
}


