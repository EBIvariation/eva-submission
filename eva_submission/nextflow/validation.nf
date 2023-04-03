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
    vcf_channel = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.vcf), file(row.fasta), file(row.report))}

    if ("vcf_check" in params.validation_tasks) {
        check_vcf_valid(vcf_channel)
    }
    if ("assembly_check" in params.validation_tasks) {
        check_vcf_reference(vcf_channel)
    }
    if ("normalisation_check" in params.validation_tasks) {
        fasta_channel = Channel.fromPath(params.vcf_files_mapping)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.fasta), file(row.report), row.assembly_accession, file(row.vcf))}
            .groupTuple(by: [0, 1, 2])
        prepare_genome(fasta_channel)
        assembly_and_vcf_channel = Channel.fromPath(params.vcf_files_mapping)
            .splitCsv(header:true)
            .map{row -> tuple(row.assembly_accession, file(row.vcf))}
            .combine(prepare_genome.out.custom_fasta, by: 0)
        normalise_vcf(assembly_and_vcf_channel)
    }
    if ("structural_variant_check" in params.validation_tasks) {
        detect_sv(vcf_channel)
    }
}


/*
* Validate the VCF file format
*/
process check_vcf_valid {
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
* Convert the genome to the same naming convention as the VCF
*/
process prepare_genome {

    input:
    tuple path(fasta), path(report), val(assembly_accession), path(vcf_files)

    output:
    tuple val(assembly_accession), path("${fasta.getSimpleName()}_custom.fa"), emit: custom_fasta

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.rename_contigs_from_insdc_in_assembly \
    --assembly_accession $assembly_accession --assembly_fasta $fasta --custom_fasta ${fasta.getSimpleName()}_custom.fa \
    --assembly_report $report --vcf_files $vcf_files
    """
}


/*
* Normalise the VCF files
*/
process normalise_vcf {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    tuple val(assembly_accession), path(vcf_file), path(fasta)

    output:
    path "normalised_vcfs/*.gz", emit: normalised_vcf
    path "normalised_vcfs/*.log", emit: normalisation_log

    script:
    """
    trap 'if [[ \$? == 1 || \$? == 139 || \$? == 255 ]]; then exit 0; fi' EXIT

    mkdir normalised_vcfs
    if [[ $vcf_file =~ \\.gz\$ ]]
    then
        $params.executable.bcftools norm --no-version -f $fasta -O z -o normalised_vcfs/$vcf_file $vcf_file 2> normalised_vcfs/${vcf_file.getBaseName()}_bcftools_norm.log
    else
        $params.executable.bcftools norm --no-version -f $fasta -O z -o normalised_vcfs/${vcf_file}.gz $vcf_file 2> normalised_vcfs/${vcf_file}_bcftools_norm.log
    fi
    """
}

/*
 * Detect the structural variant in VCF
 */
process detect_sv {
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
