#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --valid_vcfs                csv file with the mappings for vcf file, assembly accession, fasta, assembly report,
                                        analysis_accession, db_name, vep version, vep cache version, aggregation
            --project_accession         project accession
            --load_job_props            variant load job-specific properties, passed as a map
            --acc_import_job_props      import accession job-specific properties, passed as a map
            --eva_pipeline_props        main properties file for eva pipeline
            --annotation_only           whether to only run annotation job
            --project_dir               project directory
            --logs_dir                  logs directory
    """
}

params.valid_vcfs = null
params.vep_path = null
params.project_accession = null
params.load_job_props = null
params.eva_pipeline_props = null
params.annotation_only = false
params.project_dir = null
params.logs_dir = null
// executables
params.executable = ["bgzip": "bgzip"]
// java jars
params.jar = ["eva_pipeline": "eva_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test inputs
if (!params.valid_vcfs || !params.vep_path || !params.project_accession || !params.load_job_props || !params.eva_pipeline_props || !params.project_dir || !params.logs_dir) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.vep_path) log.warn('Provide path to VEP installations using --vep_path')
    if (!params.project_accession) log.warn('Provide project accession using --project_accession')
    if (!params.load_job_props) log.warn('Provide job-specific properties using --load_job_props')
    if (!params.eva_pipeline_props) log.warn('Provide an EVA Pipeline properties file using --eva_pipeline_props')
    if (!params.project_dir) log.warn('Provide project directory using --project_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

unmerged_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name, row.vep_version, row.vep_cache_version, row.vep_species, row.aggregation)}


/*
 * Create properties files for load.
 */
process create_properties {
    input:
    tuple vcf_file, fasta, analysis_accession, db_name, vep_version, vep_cache_version, vep_species, aggregation from unmerged_vcfs

    output:
    path "load_${vcf_file.getFileName()}.properties" into variant_load_props

    exec:
    props = new Properties()
    params.load_job_props.each { k, v ->
        props.setProperty(k, v.toString())
    }
    if (params.annotation_only) {
        props.setProperty("spring.batch.job.names", "annotate-variants-job")
    } else {
        props.setProperty("spring.batch.job.names", aggregation.toString() == "none" ? "genotyped-vcf-job" : "aggregated-vcf-job")
    }
    props.setProperty("input.vcf.aggregation", aggregation.toString().toUpperCase())
    props.setProperty("input.vcf", vcf_file.toRealPath().toString())
    props.setProperty("input.vcf.id", analysis_accession.toString())
    props.setProperty("input.fasta", fasta.toString())
    props.setProperty("spring.data.mongodb.database", db_name.toString())
    if (vep_version == "" || vep_cache_version == "") {
        props.setProperty("annotation.skip", "true")
    } else {
        props.setProperty("annotation.skip", "false")
        props.setProperty("app.vep.version", vep_version.toString())
        props.setProperty("app.vep.path", "${params.vep_path}/ensembl-vep-release-${vep_version}/vep")
        props.setProperty("app.vep.cache.version", vep_cache_version.toString())
        props.setProperty("app.vep.cache.species", vep_species.toString())
    }
    // need to explicitly store in workDir so next process can pick it up
    // see https://github.com/nextflow-io/nextflow/issues/942#issuecomment-441536175
    props_file = new File("${task.workDir}/load_${vcf_file.getFileName()}.properties")
    props_file.createNewFile()
    props_file.newWriter().withWriter { w ->
        props.each { k, v ->
            w.write("$k=$v\n")
        }
    }
    // make a copy for debugging purposes
    new File("${params.project_dir}/load_${vcf_file.getFileName()}.properties") << props_file.asWritable()
}


/*
 * Load into variant db.
 */
process load_vcf {
    clusterOptions {
        log_filename = variant_load_properties.getFileName().toString()
        log_filename = log_filename.substring(5, log_filename.indexOf('.properties'))
        return "-o $params.logs_dir/pipeline.${log_filename}.log \
                -e $params.logs_dir/pipeline.${log_filename}.err"
    }

    input:
    path variant_load_properties from variant_load_props

    memory '5 GB'

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=$variant_load_properties
    """
}


vcf_files_list = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), row.db_name)}


/*
 * Create properties files for Accession Import Job.
 */
process create_properties_for_acc_import_job {
    input:
    tuple vcf_file, db_name from vcf_files_list

    output:
    path "${acc_import_property_file_name}" into accession_import_props

    exec:
    props = new Properties()
    params.acc_import_job_props.each { k, v ->
        props.setProperty(k, v.toString())
    }

    accessioned_report_name = vcf_file.getFileName().toString().replace('.vcf','.accessioned.vcf')

    props.setProperty("input.accession.report", "${params.project_dir}/60_eva_public/${accessioned_report_name}")
    props.setProperty("spring.batch.job.names", "accession-import-job")
    props.setProperty("spring.data.mongodb.database", db_name.toString())

    // need to explicitly store in workDir so next process can pick it up
    // see https://github.com/nextflow-io/nextflow/issues/942#issuecomment-441536175
    acc_import_property_file_name = "acc_import_${accessioned_report_name}.properties"
    props_file = new File("${task.workDir}/${acc_import_property_file_name}")
    props_file.createNewFile()
    props_file.newWriter().withWriter { w ->
        props.each { k, v ->
            w.write("$k=$v\n")
        }
    }
    // make a copy for debugging purposes
    new File("${params.project_dir}/${acc_import_property_file_name}") << props_file.asWritable()
}



/*
 * Import Accession Into Variant warehouse
 */
process import_accession{
    clusterOptions {
        log_filename = accession_import_properties.getFileName().toString()
        log_filename = log_filename.substring(11, log_filename.indexOf('.properties'))
        return "-o $params.logs_dir/pipeline.${log_filename}.log \
                -e $params.logs_dir/pipeline.${log_filename}.err"
    }

    input:
    path accession_import_properties from accession_import_props

    memory '5 GB'

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.eva_pipeline_props --parameters.path=$accession_import_properties
    """

}