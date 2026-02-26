#!/usr/bin/env nextflow

nextflow.enable.dsl=2


def helpMessage() {
    log.info"""
    Deprecate submitted variants and drop study from mongodb.

    Inputs:
            --valid_deprecations         csv file with columns: assembly_accession, variant_id_file, db_name, deprecation_properties_file
            --project_accession          project accession to drop from the variant warehouse
            --drop_study_props           properties file for drop-study-job
            --logs_dir                   logs directory
    """
}

params.valid_deprecations = null
params.project_accession = null
params.drop_study_props = null
params.logs_dir = null
// java jars
params.jar = ["deprecate": "deprecate", "eva_pipeline": "eva_pipeline"]
// deprecation tasks
params.tasks = ['deprecate_variants', 'drop_study']
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

workflow {

    if ('deprecate_variants' in params.tasks) {
        deprecation_channel = Channel.fromPath(params.valid_deprecations)
            .splitCsv(header: true)
            .map { row -> tuple(row.assembly_accession, file(row.variant_id_file), row.db_name, file(row.deprecation_properties_file)) }
        deprecate_submitted_variants(deprecation_channel)
    }

    if ('drop_study' in params.tasks) {
        db_name_channel = Channel.fromPath(params.valid_deprecations)
            .splitCsv(header: true)
            .map { row -> row.db_name }
            .unique()

        if ('deprecate_variants' in params.tasks) {
            drop_study(db_name_channel, deprecate_submitted_variants.out.deprecation_complete.collect())
        } else {
            drop_study(db_name_channel, Channel.value([]))
        }
    }
}


/*
 * Deprecate submitted variants (SSIDs) from file, per assembly
 */
process deprecate_submitted_variants {
    label 'long_time', 'med_mem'

    input:
    tuple val(assembly_accession), path(variant_id_file), val(db_name), path(deprecation_properties_file)

    output:
    val true, emit: deprecation_complete

    script:
    def log = "$params.logs_dir/deprecate.${assembly_accession}.log"
    """
    java -Xmx4G -jar $params.jar.deprecate --spring.config.location=file:$deprecation_properties_file > ${log} 2>&1
    """
}


/*
 * Drop the study from the variant warehouse MongoDB database
 */
process drop_study {
    label 'long_time', 'med_mem'

    input:
    val db_name
    val deprecation_complete

    output:
    val true, emit: drop_complete

    script:
    """
    java -Xmx${task.memory.toGiga()-1}G -jar $params.jar.eva_pipeline \
        --spring.config.location=file:$params.drop_study_props \
        --spring.batch.job.names=drop-study-job \
        --input.study.id=$params.project_accession \
        --spring.data.mongodb.database=$db_name \
        > $params.logs_dir/drop_study.${db_name}.log 2>&1
    """
}
