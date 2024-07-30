#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Validate a set of VCF files and metadata to check if they are valid to be submitted to EVA.

    Inputs:
            --vcf_files_mapping     csv file with the mappings for vcf files, fasta and assembly report
            --output_dir            output_directory where the reports will be output
    """
}

params.vcf_files_mapping = null
params.output_dir = null
// executables
params.executable = ["vcf_assembly_checker": "vcf_assembly_checker", "vcf_validator": "vcf_validator", "bgzip": "bgzip"]
// validation tasks
params.validation_tasks = ["assembly_check", "vcf_check", "normalisation_check", "structural_variant_check"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files_mapping || !params.output_dir) {
    if (!params.vcf_files_mapping)    log.warn('Provide a csv file with the mappings (vcf, fasta, assembly report) --vcf_files_mapping')
    if (!params.output_dir)    log.warn('Provide an output directory where the reports will be copied using --output_dir')
    exit 1, helpMessage()
}


workflow {
    vcf_info_ch = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf), file(row.fasta), file(row.report))}
    vcf_info_acc_ch = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf), row.assembly_accession)}

    if ("vcf_check" in params.validation_tasks) {
        check_vcf_valid(vcf_info_ch)
    }
    if ("assembly_check" in params.validation_tasks) {
        check_vcf_reference(vcf_info_ch)
    }
    if ("structural_variant_check" in params.validation_tasks) {
        detect_sv(vcf_info_ch)
    }
    if ("naming_convention_check" in params.validation_tasks) {
        detect_naming_convention(vcf_info_acc_ch)
    }
}


/*
* Validate the VCF file format
*/
process check_vcf_valid {
    label 'long_time', 'med_mem'

    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    tuple path(vcf), path(fasta), path(report)

    output:
    path "vcf_format/*.errors.*.db", emit: vcf_validation_db
    path "vcf_format/*.errors.*.txt", emit: vcf_validation_txt
    path "vcf_format/*.vcf_format.log", emit: vcf_validation_log

    script:
    """
    trap 'if [[ \$? == 1 ]]; then exit 0; fi' EXIT

    mkdir -p vcf_format
    $params.executable.vcf_validator -i $vcf  -r database,text -o vcf_format --require-evidence > vcf_format/${vcf}.vcf_format.log 2>&1
    """
}


/*
* Validate the VCF reference allele
*/
process check_vcf_reference {
    label 'long_time', 'med_mem'

    publishDir "$params.output_dir",
            overwrite: true,
            mode: "copy"

    input:
    tuple path(vcf), path(fasta), path(report)

    output:
    path "assembly_check/*valid_assembly_report*", emit: vcf_assembly_valid
    path "assembly_check/*text_assembly_report*", emit: assembly_check_report
    path "assembly_check/*.assembly_check.log", emit: assembly_check_log

    script:
    """
    trap 'if [[ \$? == 1 || \$? == 139 ]]; then exit 0; fi' EXIT

    mkdir -p assembly_check
    $params.executable.vcf_assembly_checker -i $vcf -f $fasta -a $report -r summary,text,valid  -o assembly_check --require-genbank > assembly_check/${vcf}.assembly_check.log 2>&1
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
    --vcf_file $vcf_file --output_vcf_file_with_sv sv_check/${vcf_file.getSimpleName()}_sv_list.vcf \
    > sv_check/${vcf_file.getSimpleName()}_sv_check.log 2>&1
    $params.executable.bgzip -c sv_check/${vcf_file.getSimpleName()}_sv_list.vcf > sv_check/${vcf_file.getSimpleName()}_sv_list.vcf.gz
    rm sv_check/${vcf_file.getSimpleName()}_sv_list.vcf
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
    tuple path(vcf_file), accession

    output:
    path "naming_convention_check/*_naming_convention.yml", emit: nc_check_yml

    script:
    """
    mkdir -p naming_convention_check

    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.detect_contigs_naming_convention.py \
    --vcf_files $vcf_file --assembly_accession $accession --output_yaml naming_convention_check/${vcf_file.getSimpleName()}_naming_convention.yml
    """
}
