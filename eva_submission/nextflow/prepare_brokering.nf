#!/usr/bin/env nextflow

nextflow.enable.dsl=2
def helpMessage() {
    log.info"""
    Prepare vcf file ready to be broker to ENA.

    Inputs:
            --vcf_files_mapping     csv file with the mappings for vcf files, fasta and assembly report
            --output_dir            output_directory where the final will be written

    """
}

params.vcf_files_mapping = null
// executables
params.executable = ["md5sum", "tabix", "bgzip", "bcftools"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.vcf_files_mapping || !params.output_dir) {
    if (!params.vcf_files_mapping)    log.warn('Provide a csv file with the mappings (vcf, fasta, assembly report) --vcf_files_mapping')
    if (!params.output_dir)           log.warn('Provide an output directory where the output file will be copied using --output_dir')

    exit 1, helpMessage()
}


workflow {
    vcf_channel = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> file(row.vcf)}
    compress_vcf(vcf_channel)
    csi_index_vcf(compress_vcf.out.compressed_vcf_tuple)

    fasta_channel = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.fasta), file(row.report), row.assembly_accession, file(row.vcf))}
        .groupTuple(by: [0, 1, 2])
    prepare_genome(fasta_channel)

    assembly_and_vcf_channel = Channel.fromPath(params.vcf_files_mapping)
        .splitCsv(header:true)
        .map{row -> tuple(row.assembly_accession, file(row.vcf))}
        .combine(prepare_genome.out.custom_fasta, by: 0)         // Join based on the assembly
        .map{tuple(it[1].name, it[0], it[2])}                    // reorder to get the name of the input file first
        .combine(compress_vcf.out.compressed_vcf_tuple, by: 0)   // Join VCF based on the name of the input file
        .combine(csi_index_vcf.out.csi_indexed_vcf_tuple, by: 0) // Join CSI based on the name of the input file
    normalise_vcf(assembly_and_vcf_channel)

    md5_vcf_and_index(normalise_vcf.out.normalised_vcf, normalise_vcf.out.normalised_vcf_index)

}


/*
* Compress the VCF file
*/
process compress_vcf {

    input:
    path vcf_file

    output:
    tuple val(vcf_file.name), path("output/*.gz"), emit: compressed_vcf_tuple

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

    input:
    tuple val(input_vcf), path(compressed_vcf)

    output:
    tuple val(input_vcf), path("${compressed_vcf}.csi"), emit: csi_indexed_vcf_tuple

    """
    $params.executable.bcftools index -c $compressed_vcf
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
            mode: "copy",
            saveAs: { fn -> fn.substring(fn.lastIndexOf('/')+1) }

    input:
    tuple val(filename), val(assembly_accession),  path(fasta), path(vcf_file), path(csi_file)

    output:
    path "normalised_vcfs/*.gz", emit: normalised_vcf
    path "normalised_vcfs/*.csi", emit: normalised_vcf_index
    path "normalised_vcfs/*.log", emit: normalisation_log

    script:
    """
    mkdir normalised_vcfs
    $params.executable.bcftools norm --no-version -cw -f $fasta -O z -o normalised_vcfs/$vcf_file $vcf_file 2> normalised_vcfs/${vcf_file.getBaseName()}_bcftools_norm.log
    $params.executable.bcftools index -c normalised_vcfs/$vcf_file
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
    path vcf
    path index

    output:
    path "${vcf}.md5", emit: vcf_md5
    path "${index}.md5", emit: index_md5

    """
    $params.executable.md5sum ${vcf} > ${vcf}.md5
    $params.executable.md5sum ${index} > ${index}.md5
    """
}
