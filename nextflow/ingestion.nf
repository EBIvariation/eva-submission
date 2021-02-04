#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Ingest VCF files to MongoDB.

    Inputs:
            --vcf_files           list of vcf files that are meant to be validated
            --output_dir         output_directory where the reports will be ouptut
    """
}

# general parameters
params.assembly = null
params.taxonomy = null
params.project = null
params.aggregation = null // BASIC or NONE
params.fasta = null
params.report = null
params.instance = null // for now must be manually determined
// executables
params.executable =["bcf_tools": "bcf_tools", "create_accession_props": "create_accession_props"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline", "eva_pipeline": "eva_pipeline"]
// help
params.help = null


// Show help message
if (params.help) exit 0, helpMessage()


/*
* Create accession props file
*/
process create_accession_props {
    output:
        path "accession.properties" into accession_props

    """
    $params.executable.create_accession_props $params.assembly $params.taxonomy $params.project $params.aggregation $params.fasta $params.instance $params.report
    """
}


/*
* Accession VCF
*/
process accession_vcf {
    input:
        path "merged.vcf" from merged_vcf
        path "accession.properties" from accession_props

    output:
        some output logs...

    """
    java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=*application_properties_name*
    """
}

/*
* Create mongo load props file
*/
process create_load_props {
    """
    # TODO is this here or in the python?
    """
}


/*
* Load into variant db.
*/
process load_vcf {
    input:
        path "merged.vcf" from merged_vcf

    output:
        some output logs...

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:/nfs/production3/eva/software/eva-pipeline/application-production.properties --parameters.path=load-genotyped-vcf.properties
    """
}
