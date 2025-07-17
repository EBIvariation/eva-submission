#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Validate a set of VCF files and metadata to check if they are valid to be submitted to EVA.

    Inputs:
            --vcf_files_mapping     csv file with the mappings for vcf files, fasta and assembly report
            --output_dir            output_directory where the reports will be output
            --metadata_json         metadata JSON to be validated with eva-sub-cli (optional)
    """
}

params.vcf_files_mapping = null
params.output_dir = null
params.metadata_json = null
// executables
params.executable = ["vcf_assembly_checker": "vcf_assembly_checker", "vcf_validator": "vcf_validator", "bgzip": "bgzip",
                     "eva_sub_cli": "eva_sub_cli", "sub_cli_env": "sub_cli_env"]
// validation tasks
params.validation_tasks = ["eva_sub_cli", "structural_variant_check", "naming_convention_check"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files_mapping || !params.output_dir || !params.metadata_json) {
    if (!params.vcf_files_mapping)    log.warn('Provide a csv file with the mappings (vcf, fasta, assembly report) --vcf_files_mapping')
    if (!params.output_dir)    log.warn('Provide an output directory where the reports will be copied using --output_dir')
    if (!params.metadata_json)    log.warn('Provide a json file containing the metadata and location of files using --metadata_json')
    exit 1, helpMessage()
}


workflow {
    vcf_info_ch = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf), file(row.fasta), file(row.report))}
    vcf_info_acc_ch = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf), row.assembly_accession)}

    if ("eva_sub_cli" in params.validation_tasks) {
            run_eva_sub_cli()
    }
    if ("structural_variant_check" in params.validation_tasks) {
        detect_sv(vcf_info_ch)
    }
    if ("naming_convention_check" in params.validation_tasks) {
        detect_naming_convention(vcf_info_acc_ch)
    }
}


/*
 * Run eva-sub-cli
 */
process run_eva_sub_cli {
    label 'long_time', 'med_mem'

    publishDir "$params.output_dir",
        overwrite: false,
        mode: "copy"

    output:
    path "validation_results.yaml", emit: eva_sub_cli_results
    path "validation_output/report.html", emit: eva_sub_cli_report
    path "validation_output/report.txt", emit: eva_sub_cli_text_report

    script:
    """
    source $params.executable.sub_cli_env
    $params.executable.eva_sub_cli --submission_dir . --metadata_json ${params.metadata_json} --tasks VALIDATE
    """
}


/*
 * Detect the structural variant in VCF
 */
process detect_sv {
    label 'default_time', 'med_mem'

    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    tuple path(vcf_file), path(fasta), path(report)

    output:
    path "sv_check/*_sv_check.log", emit: sv_check_log
    path "sv_check/*_sv_list.vcf.gz", emit: sv_list_vcf

    script:
    """
    mkdir -p sv_check

    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.structural_variant_detection \
    --vcf_file $vcf_file --output_vcf_file_with_sv sv_check/${vcf_file.getBaseName()}_sv_list.vcf \
    > sv_check/${vcf_file.getBaseName()}_sv_check.log 2>&1
    $params.executable.bgzip -c sv_check/${vcf_file.getBaseName()}_sv_list.vcf > sv_check/${vcf_file.getBaseName()}_sv_list.vcf.gz
    rm sv_check/${vcf_file.getBaseName()}_sv_list.vcf
    """
}


/*
 * Detect the naming convention in VCF
 */
process detect_naming_convention {
    label 'default_time', 'med_mem'

    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    tuple path(vcf_file), val(accession)

    output:
    path "naming_convention_check/*_naming_convention.yml", emit: nc_check_yml

    script:
    """
    mkdir -p naming_convention_check

    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.detect_contigs_naming_convention \
    --vcf_files $vcf_file --assembly_accession $accession --output_yaml naming_convention_check/${vcf_file.getBaseName()}_naming_convention.yml
    """
}
