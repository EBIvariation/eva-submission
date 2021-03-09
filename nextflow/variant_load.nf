#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --valid_vcfs            valid vcfs to load
            --needs_merge           whether horizontal merge is required (false by default)
            --project_accession     project accession
            --job_props             job-specific properties, passed as a map
            --eva_pipeline_props    main properties file for eva pipeline
            --logs_dir              logs directory
    """
}

params.valid_vcfs = null
params.needs_merge = null
params.project_accession = null
params.job_props = null
params.eva_pipeline_props = null
params.logs_dir = null
// executables
params.executable = ["bgzip": "bgzip", "bcftools": "bcftools"]
// java jars
params.jar = ["eva_pipeline": "eva_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test inputs
if (!params.valid_vcfs || !params.project_accession || !params.job_props || !params.eva_pipeline_props || !params.logs_dir) {
    if (!params.valid_vcfs) log.warn('Provide validated vcfs using --valid_vcfs')
    if (!params.project_accession) log.warn('Provide project accession using --project_accession')
    if (!params.job_props) log.warn('Provide job-specific properties using --job_props')
    if (!params.eva_pipeline_props) log.warn('Provide an EVA Pipeline properties file using --eva_pipeline_props')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

// Valid vcfs are redirected to merge step or directly to load
// See https://nextflow-io.github.io/patterns/index.html#_skip_process_execution
(vcfs_to_merge, unmerged_vcfs) = (
    params.needs_merge
    ? [Channel.from(params.valid_vcfs), Channel.empty()]
    : [Channel.empty(), Channel.fromPath(params.valid_vcfs)] )


/*
 * Merge VCFs horizontally, i.e. by sample.
 */
process merge_vcfs {
    input:
    path file_list from vcfs_to_merge.collectFile(name: 'all_files.list', newLine: true)

    output:
    path "${params.project_accession}_merged.vcf" into merged_vcf

    """
    $params.executable.bcftools merge --merge all --file-list $file_list --threads 3 -o ${params.project_accession}_merged.vcf
    """
}


/*
 * Compress merged VCF file.
 */
process compress_vcf {
    input:
    path vcf_file from merged_vcf

    output:
    path "${vcf_file}.gz" into compressed_vcf

    """
    $params.executable.bgzip -c $vcf_file > ${vcf_file}.gz
    """
}


/*
 * Create properties files for load.
 */
process create_properties {
    input:
    // note one of these channels is always empty
    path vcf_file from unmerged_vcfs.mix(compressed_vcf)

    output:
    path "load_${vcf_file}.properties" into variant_load_props

    exec:
    props = new Properties()
    props.putAll(params.job_props)
    props.setProperty("input.vcf", vcf_file.toString())
    // need to store in workDir so next process can pick it up
    // this needs to happen explicitly in exec (as opposed to script)
    props_file = new File("${task.workDir}/load_${vcf_file}.properties")
    props_file.createNewFile()
    props_file.withWriter { w ->
	props.store(w, null)
    }
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
