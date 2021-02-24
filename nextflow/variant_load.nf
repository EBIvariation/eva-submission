#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --variant_load_props    properties files for variant load
            --eva_pipeline_props    main properties file for eva pipeline
            --logs_dir              logs directory
    """
}

params.variant_load_props = null
params.eva_pipeline_props = null
params.logs_dir = null
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
        path variant_load_properties from variant_load_props

    """
    filename=\$(basename $variant_load_properties)
    filename=\${filename%.*}
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=$variant_load_properties \
        > $params.logs_dir/pipeline.\${filename}.log \
        2> $params.logs_dir/pipeline.\${filename}.err
    """
}
