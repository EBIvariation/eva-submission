#!/usr/bin/env nextflow


nextflow.enable.dsl=2

include { copy_to_ftp } from './common_processes.nf'

def helpMessage() {
    log.info"""
    Only copy to public FTP.

    Inputs:
            --valid_vcfs                csv file with the mappings for vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name
            --project_accession         project accession
            --public_ftp_dir            public FTP directory
            --public_dir                directory for files to be made public
    """
}

params.valid_vcfs = null
params.project_accession = null
params.public_ftp_dir = null
params.public_dir = null

// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.valid_vcfs || !params.public_ftp_dir || !params.public_dir ) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.public_ftp_dir) log.warn('Provide public FTP directory using --public_ftp_dir')
    if (!params.public_dir) log.warn('Provide public directory using --public_dir')
    exit 1, helpMessage()
}

workflow {
    vcf_channel = Channel.fromPath(params.valid_vcfs)
        .splitCsv(header:true)
        .map{row -> file(row.vcf_file)}
    copy_to_ftp(vcf_channel.toList(), channel.empty().toList())
}
