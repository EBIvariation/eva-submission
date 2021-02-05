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
        some output logs...

    """
    java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=accession.properties
    """
}

// TODO accessioned files in 60_eva_public need to be compressed & moved to FTP folder

/*
* Load into variant db.
*/
process load_vcf {
    input:
        path "variant_load.properties" from variant_load_props

    output:
        some output logs...

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=variant_load.properties
    """
}
