#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --valid_vcfs                csv file with the mappings for vcf file, assembly accession, fasta, assembly report,
                                        analysis_accession, db_name, vep version, vep cache version, aggregation
            --project_accession         project accession
            --load_job_props            properties file for variant load job
            --acc_import_job_props      properties file for accession import job
            --annotation_only           whether to only run annotation job
            --taxonomy                  taxonomy id
            --project_dir               project directory
            --logs_dir                  logs directory
    """
}

params.valid_vcfs = null
params.vep_path = null
params.project_accession = null
params.load_job_props = null
params.acc_import_job_props = null
params.annotation_only = false
params.taxonomy = null
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
if (!params.valid_vcfs || !params.vep_path || !params.project_accession || !params.taxonomy || !params.load_job_props || !params.acc_import_job_props || !params.project_dir || !params.logs_dir) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.vep_path) log.warn('Provide path to VEP installations using --vep_path')
    if (!params.project_accession) log.warn('Provide project accession using --project_accession')
    if (!params.taxonomy) log.warn('Provide taxonomy id using --taxonomy')
    if (!params.load_job_props) log.warn('Provide path to variant load job properties file --load_job_props')
    if (!params.acc_import_job_props) log.warn('Provide path to accession import job properties file using --acc_import_job_props')
    if (!params.project_dir) log.warn('Provide project directory using --project_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}


workflow {
    unmerged_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name, row.vep_version, row.vep_cache_version, row.vep_species, row.aggregation)}
    load_vcf(unmerged_vcfs)

    if (params.taxonomy != 9606) {
        vcf_files_list = Channel.fromPath(params.valid_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file), row.db_name)}
        import_accession(vcf_files_list, load_vcf.out.variant_load_complete)
    }
}

/*
 * Load into variant db.
 */
process load_vcf {
    clusterOptions {
        log_filename = vcf_file.getFileName().toString()
        return "-o $params.logs_dir/pipeline.${log_filename}.log \
                -e $params.logs_dir/pipeline.${log_filename}.err"
    }

    input:
    tuple val(vcf_file), val(fasta), val(analysis_accession), val(db_name), val(vep_version), val(vep_cache_version), val(vep_species), val(aggregation)

    output:
    val true, emit: variant_load_complete

    memory '5 GB'

    script:
    def pipeline_parameters = ""

    if (params.annotation_only) {
        pipeline_parameters += " --spring.batch.job.names=annotate-variants-job"
    } else if(aggregation.toString() == "none"){
        pipeline_parameters += " --spring.batch.job.names=genotyped-vcf-job"
    } else{
        pipeline_parameters += " --spring.batch.job.names=aggregated-vcf-job"
    }

    pipeline_parameters += " --input.vcf.aggregation=" + aggregation.toString().toUpperCase()
    pipeline_parameters += " --input.vcf=" + vcf_file.toRealPath().toString()
    pipeline_parameters += " --input.vcf.id=" + analysis_accession.toString()
    pipeline_parameters += " --input.fasta=" + fasta.toString()

    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()

    if (vep_version == "" || vep_cache_version == "") {
        pipeline_parameters += " --annotation.skip=true"
    } else {
        pipeline_parameters += " --annotation.skip=false"
        pipeline_parameters += " --app.vep.version=" + vep_version.toString()
        pipeline_parameters += " --app.vep.path=" + "${params.vep_path}/ensembl-vep-release-${vep_version}/vep"
        pipeline_parameters += " --app.vep.cache.version=" + vep_cache_version.toString()
        pipeline_parameters += " --app.vep.cache.species=" + vep_species.toString()
    }

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.load_job_props $pipeline_parameters
    """
}


/*
 * Import Accession Into Variant warehouse
 */
process import_accession {
    clusterOptions {
        log_filename = vcf_file.getFileName().toString()
        return "-o $params.logs_dir/pipeline.${log_filename}.log \
                -e $params.logs_dir/pipeline.${log_filename}.err"
    }

    input:
    tuple val(vcf_file), val(db_name)
    val variant_load_output

    memory '5 GB'

    script:
    def pipeline_parameters = ""

    accessioned_report_name = vcf_file.getFileName().toString().replace('.vcf','.accessioned.vcf')
    pipeline_parameters += " --input.accession.report=" + "${params.project_dir}/60_eva_public/${accessioned_report_name}"
    pipeline_parameters += " --spring.batch.job.names=accession-import-job"
    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()


    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.acc_import_job_props $pipeline_parameters
    """
}
