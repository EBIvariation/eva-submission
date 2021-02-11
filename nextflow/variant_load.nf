#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Accession and ingest variant files.

    Inputs:
            --variant_load_props    properties files for variant load
            --eva_pipeline_props    main properties file for eva pipeline
    """
}

params.variant_load_props = null
params.eva_pipeline_props = null
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

variant_load_props = Channel.fromPath(params.variant_load_props)


/*
* Load into variant db.
*/
process load_vcf {
    input:
        path "variant_load.properties" from variant_load_props

    output:
        path "00_logs/pipeline.*.log" into pipeline_log
        path "00_logs/pipeline.*.err" into pipeline_err

    """
    filename=$(basename variant_load.properties)
    filename="${filename%.*}"
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=variant_load.properties \
        > 00_logs/pipeline.${filename}.log \
        2> 00_logs/pipeline.${filename}.err
    """
}
