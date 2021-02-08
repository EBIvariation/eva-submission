#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Accession and ingest variant files.

    Inputs:
            --accession_props
            --variant_load_props
            --eva_pipeline_props
            --output_dir         output_directory where the reports will be output
    """
}

params.accession_props = null
params.variant_load_props = null
params.eva_pipeline_props = null
params.output_dir = null
// executables
params.executable =["bcf_tools": "bcf_tools", "create_accession_props": "create_accession_props"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline", "eva_pipeline": "eva_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.accession_props || !params.variant_load_props || !params.eva_pipeline_props || !params.output_dir) {
    if (!params.accession_props)    log.warn('Provide an accessions properties file using --accession_props')
    if (!params.variant_load_props)  log.warn('Provide a variant load properties file using --variant_load_props')
    if (!params.eva_pipeline_props)    log.warn('Provide an EVA Pipeline properties file using --eva_pipeline_props')
    if (!params.output_dir)    log.warn('Provide an output directory where the reports will be copied using --output_dir')
    exit 1, helpMessage()
}

accession_props = Channel.fromPath(params.accession_props)
variant_load_props = Channel.fromPath(params.variant_load_props)


/*
* Accession VCF
*/
process accession_vcf {
    input:
        path "accession.properties" from accession_props

    output:
        path "00_logs/accessioning.*.log" into accessioning_log
        path "00_logs/accessioning.*.err" into accessioning_err

    """
    filename=$(basename accession.properties)
    filename="${filename%.*}"
    # TODO still confused as to whether this will run on the lsf instance properly...
    java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=accession.properties \
        > 00_logs/accessioning.${filename}.log \
        2> 00_logs/accessioning.${filename}.err
    # TODO accessioned files in 60_eva_public need to be compressed & moved to FTP folder
    """
}


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
