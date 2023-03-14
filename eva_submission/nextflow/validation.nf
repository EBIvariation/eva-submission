#!/usr/bin/env nextflow

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


// vcf files are used multiple times
Channel.fromPath(params.vcf_files_mapping)
    .splitCsv(header:true)
    .map{row -> tuple(file(row.vcf), file(row.fasta), file(row.report))}
    .into{vcf_channel1; vcf_channel2; vcf_channel3; vcf_channel4}

Channel.fromPath(params.vcf_files_mapping)
    .splitCsv(header:true)
    .map{row -> tuple(file(row.fasta), file(row.report), row.assembly_accession, file(row.vcf))}
    .groupTuple(by: [0, 1, 2])
    .set{fasta_channel}

/*
* Validate the VCF file format
*/
process check_vcf_valid {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    set file(vcf), file(fasta), file(report) from vcf_channel1

    when:
    "vcf_check" in params.validation_tasks

    output:
    path "vcf_format/*.errors.*.db" into vcf_validation_db
    path "vcf_format/*.errors.*.txt" into vcf_validation_txt
    path "vcf_format/*.vcf_format.log" into vcf_validation_log

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
    set file(vcf), file(fasta), file(report) from vcf_channel2

    output:
    path "assembly_check/*valid_assembly_report*" into vcf_assembly_valid
    path "assembly_check/*text_assembly_report*" into assembly_check_report
    path "assembly_check/*.assembly_check.log" into assembly_check_log

    when:
    "assembly_check" in params.validation_tasks

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
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    set file(fasta), file(report), assembly_accession, file(vcf_files) from fasta_channel


    output:
    tuple assembly_accession, path("${fasta.getSimpleName()}_custom.fa") into custom_fasta


    when:
    "normalisation_check" in params.validation_tasks

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.rename_contigs_from_insdc_in_assembly \
    --assembly_accession $assembly_accession --assembly_fasta $fasta --custom_fasta ${fasta.getSimpleName()}_custom.fa \
    --assembly_report $report --vcf_files $vcf_files
    """
}


Channel.fromPath(params.vcf_files_mapping)
    .splitCsv(header:true)
    .map{row -> tuple(row.assembly_accession, file(row.vcf))}
    .combine(custom_fasta, by: 0)
    .set{assembly_and_vcf_channel}



/*
* Normalise the VCF files
*/
process normalise_vcf {
    publishDir "$params.output_dir",
            overwrite: false,
            mode: "copy"

    input:
    set assembly_accession, file(vcf_file), file(fasta) from assembly_and_vcf_channel

    output:
    path "normalised_vcfs/*.gz" into normalised_vcf
    path "normalised_vcfs/*.log" into normalisation_log

    when:
    "normalisation_check" in params.validation_tasks

    """
    trap 'if [[ \$? == 1 || \$? == 139 ]]; then exit 0; fi' EXIT

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
    set file(vcf_file), file(fasta), file(report) from vcf_channel3

    output:
    path "sv_check/*_sv_check.log" into sv_check_log
    path "sv_check/*_sv_list.vcf.gz" into sv_list_vcf


    when:
    "structural_variant_check" in params.validation_tasks

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