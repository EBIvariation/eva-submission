#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --variant_load_props    properties files for variant load
            --eva_pipeline_props    main properties file for eva pipeline
            --project_accession     project accession
            --logs_dir              logs directory
    """
}

params.variant_load_props = null
params.eva_pipeline_props = null
params.project_accession = null
params.logs_dir = null
// executables
params.executable = ["bgzip": "bgzip", "bcftools": "bcftools"]
// java jars
params.jar = ["eva_pipeline": "eva_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.variant_load_props || !params.eva_pipeline_props) {
    if (!params.variant_load_props) log.warn('Provide a variant load properties file using --variant_load_props')
    if (!params.eva_pipeline_props) log.warn('Provide an EVA Pipeline properties file using --eva_pipeline_props')
    exit 1, helpMessage()
}

//variant_load_props = Channel.fromPath(params.variant_load_props)


// valid vcfs redirected to merge step or directly to load
Channel.from(params.valid_vcfs)
    .branch {
	vcfs_to_merge: params.needs_merge
	vcfs_to_load: true
    }


/*
 * Merge VCFs by sample.
 */
process merge_vcfs {
    input:
    path file_list from vcfs_to_merge.collectFile('all_files.list', newLine: true)

    output:
    path ${params.project_accession}_merged.vcf into merged_vcf

    """
    $params.executable.bcftools merge --merge all --file-list $file_list --threads 3 -o ${params.project_accession}_merged.vcf
    """
}


/*
 * Compress merged vcf file.
 */
process compress_vcf {
    input:
    path vcf_file from merged_vcf

    output:
    path "${vcf_file}.gz" into vcfs_to_load

    """
    $params.executable.bgzip -c $vcf_file > ${vcf_file}.gz
    """
}


process create_properties {
    publishDir params.project_dir,
	mode: 'copy'

    input:
    path vcf_file from vcfs_to_load

    output:
    path "load_${vcf_file}.properties" into variant_load_props

    """

    """
}


/*
 * Load into variant db.
 */
process load_vcf {
    input:
        path variant_load_properties from variant_load_props

    memory '5 GB'

    """
    filename=\$(basename $variant_load_properties)
    filename=\${filename%.*}
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=$variant_load_properties \
        > $params.logs_dir/pipeline.\${filename}.log \
        2> $params.logs_dir/pipeline.\${filename}.err
    """
}
