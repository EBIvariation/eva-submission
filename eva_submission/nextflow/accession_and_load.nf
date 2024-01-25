#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Accession variant files and copy to public FTP.

    Inputs:
            --valid_vcfs                csv file with the mappings for vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name
            --load_job_props            properties file for variant load job
            --acc_import_job_props      properties file for accession import job
            --annotation_only           whether to only run annotation job
            --project_accession         project accession
            --instance_id               instance id to run accessioning
            --accession_job_props       properties file for accessioning job
            --public_ftp_dir            public FTP directory
            --accessions_dir            accessions directory (for properties files)
            --public_dir                directory for files to be made public
            --logs_dir                  logs directory
            --taxonomy                  taxonomy id
    """
}

params.valid_vcfs = null
params.project_accession = null
params.instance_id = null
params.accession_job_props = null
params.public_ftp_dir = null
params.accessions_dir = null
params.public_dir = null
params.logs_dir = null
params.taxonomy = null
params.load_job_props = null
params.acc_import_job_props = null
params.annotation_only = null

// executables
params.executable = ["bcftools": "bcftools", "tabix": "tabix", "bgzip": "bgzip"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline", "eva_pipeline": "eva_pipeline"]
// ingestion tasks
params.ingestion_tasks = ["metadata_load", "accession", "variant_load", "annotation", "optional_remap_and_cluster"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.valid_vcfs || !params.project_accession || !params.instance_id || !params.accession_job_props ||\
    !params.public_ftp_dir || !params.accessions_dir || !params.public_dir || !params.logs_dir || !params.taxonomy || \
    !params.vep_path || !params.load_job_props || !params.acc_import_job_props || !params.project_dir) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.project_accession) log.warn('Provide a project accession using --project_accession')
    if (!params.instance_id) log.warn('Provide an instance id using --instance_id')
    if (!params.accession_job_props) log.warn('Provide job-specific properties using --accession_job_props')
    if (!params.taxonomy) log.warn('Provide taxonomy id using --taxonomy')
    if (!params.public_ftp_dir) log.warn('Provide public FTP directory using --public_ftp_dir')
    if (!params.accessions_dir) log.warn('Provide accessions directory using --accessions_dir')
    if (!params.public_dir) log.warn('Provide public directory using --public_dir')
    if (!params.vep_path) log.warn('Provide path to VEP installations using --vep_path')
    if (!params.load_job_props) log.warn('Provide path to variant load job properties file --load_job_props')
    if (!params.acc_import_job_props) log.warn('Provide path to accession import job properties file using --acc_import_job_props')
    if (!params.project_dir) log.warn('Provide project directory using --project_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

/*
Sequence of processes in case of:
    non-human study:
                accession_vcf -> sort_and_compress_vcf -> csi_index_vcf -> copy_to_ftp
                                                        \
                                          variant_load -> import_accession
    human study (skip accessioning):
                csi_index_vcf -> copy_to_ftp

process                     input channels
accession_vcf       ->      valid_vcfs
csi_index_vcf       ->      csi_vcfs and compressed_vcf

1. Check if the study we are working with is a human study or non-human by comparing the taxonomy_id of the study with human taxonomy_id (9606).
2. Provide values to the appropriate channels enabling them to start the corresponding processes. In case of non-human studies we want to start process
   "accession_vcf" while in case of human studies we want to start processes "csi_index_vcf".

non-human study:
  - Initialize valid_vcfs channel with value so that it can start the process "accession_vcf".
  - Initialize csi_vcfs channels as empty. This makes sure the processes "csi_index_vcf" are not started at the outset.
    These processes will only be able to start after the process "sort_and_compress_vcf" finishes and create channels compressed_vcf with values.

human study:
  - Initialize valid_vcfs channel as empty, ensuring the process "accession_vcf" is not started and in turn accessioning part is also skipped
  - Initialize csi_vcfs with values enabling them to start the processes "csi_index_vcf".
*/
workflow {

    fasta_channel = Channel.fromPath(params.valid_vcfs)
        .splitCsv(header:true)
        .map{row -> tuple(file(row.fasta), file(row.report), row.assembly_accession, file(row.vcf_file))}
        .groupTuple(by: [0, 1, 2])

    prepare_genome(fasta_channel)

    assembly_and_vcf_channel = Channel.fromPath(params.valid_vcfs)
        .splitCsv(header:true)
        .map{row -> tuple(row.assembly_accession, file(row.vcf_file), file(row.csi_file))}
        .combine(prepare_genome.out.custom_fasta, by: 0)     // Join based on the assembly
        .map{tuple(it[1].name, it[3], it[1], it[2])}         // vcf_filename, fasta_file, vcf_file, csi_file

    normalise_vcf(assembly_and_vcf_channel)
    all_accession_complete = null
    is_human_study = (params.taxonomy == 9606)
    if ("accession" in params.ingestion_tasks) {
        if (is_human_study) {
            csi_vcfs = Channel.fromPath(params.valid_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file))}
            accessioned_files_to_rm = Channel.empty()
        } else {
            normalised_vcfs_ch = Channel.fromPath(params.valid_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file).name, file(row.vcf_file), row.assembly_accession, row.aggregation, file(row.fasta), file(row.report))}
                .combine(normalise_vcf.out.vcf_tuples, by:0)     // Join based on the vcf_filename
                .map {tuple(it[0], it[6], it[2], it[3], it[4], it[5])}   // vcf_filename, normalised vcf, assembly_accession, aggregation, fasta, report
            accession_vcf(normalised_vcfs_ch)
            sort_and_compress_vcf(accession_vcf.out.accession_done)
            csi_vcfs = sort_and_compress_vcf.out.compressed_vcf
            accessioned_files_to_rm = accession_vcf.out.accessioned_filenames
            all_accession_complete = sort_and_compress_vcf.out.compressed_vcf
        }
        csi_index_vcf(csi_vcfs)
        copy_to_ftp(csi_index_vcf.out.csi_indexed_vcf.toList(), accessioned_files_to_rm.toList())
    }
    if ("variant_load" in params.ingestion_tasks) {
        normalised_vcfs_ch = Channel.fromPath(params.valid_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file).name, file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name, row.vep_version, row.vep_cache_version, row.vep_species, row.aggregation)}
                .combine(normalise_vcf.out.vcf_tuples, by:0)
                .map{tuple(it[0], it[9], it[2], it[3], it[4], it[5], it[6], it[7], it[8])}   // vcf_filename, normalised vcf, fasta, analysis_accession, db_name, vep_version, vep_cache_version, vep_species, aggregation
        load_variants_vcf(normalised_vcfs_ch)
        // Ensure that all the load are completed before the VEP and calculate statistics starts
        vep_ch = normalised_vcfs_ch
                .groupTuple(by: [2, 3, 4, 5, 6, 7] ) // group by fasta, analysis_accession, db_name, vep_version, vep_cache_version, vep_species
                .map{tuple(it[2], it[3], it[4], it[5], it[6], it[7])}
        run_vep_on_variants(vep_ch, load_variants_vcf.out.variant_load_complete.collect())
        stats_ch = normalised_vcfs_ch
                   .groupTuple(by: [3, 4, 8])  // group by analysis_accession, db_name, aggregation
                   .map{tuple(it[3], it[4], it[8], it[1])} // analysis_accession, db_name, aggregation, grouped normalised_vcf_files

        calculate_statistics_vcf(stats_ch, load_variants_vcf.out.variant_load_complete.collect())

        if (!is_human_study) {
            vcf_files_dbname = Channel.fromPath(params.valid_vcfs)
                    .splitCsv(header:true)
                    .map{row -> tuple(file(row.vcf_file), row.db_name)}
            // the vcf_files_dbname give the link between input file and all_accession_complete is to ensure the
            // accessioning has been completed
            if (all_accession_complete){
                import_accession(vcf_files_dbname, all_accession_complete, load_variants_vcf.out.variant_load_complete)
            }
        }
    }
}


/*
* Convert the genome to the same naming convention as the VCF
*/
process prepare_genome {

    input:
    tuple path(fasta), path(report), val(assembly_accession), path(vcf_files)

    output:
    tuple val(assembly_accession), path("${fasta.getSimpleName()}_custom.fa"), emit: custom_fasta

    script:
    """
    export PYTHONPATH="$params.executable.python.script_path"
    $params.executable.python.interpreter -m eva_submission.steps.rename_contigs_from_insdc_in_assembly \
    --assembly_accession $assembly_accession --assembly_fasta $fasta --custom_fasta ${fasta.getSimpleName()}_custom.fa \
    --assembly_report $report --vcf_files $vcf_files
    """
}


/*
* Normalise the VCF files
*/
process normalise_vcf {
    input:
    tuple val(vcf_filename), path(fasta), path(vcf_file), path(csi_file)

    output:
    tuple val(vcf_filename), path("normalised_vcfs/*.gz"), path("normalised_vcfs/*.csi"), emit: vcf_tuples

    script:
    """
    mkdir normalised_vcfs
    $params.executable.bcftools norm --no-version -cw -f $fasta -O z -o normalised_vcfs/$vcf_file $vcf_file 2> normalised_vcfs/${vcf_file.getBaseName()}_bcftools_norm.log
    $params.executable.bcftools index -c normalised_vcfs/$vcf_file
    """
}


/*
 * Accession VCFs
 */
process accession_vcf {
    clusterOptions "-g /accession/instance-${params.instance_id} \
                    -o $params.logs_dir/${log_filename}.log \
                    -e $params.logs_dir/${log_filename}.err"

    memory '6.7 GB'

    input:
    tuple val(vcf_filename), val(vcf_file), val(assembly_accession), val(aggregation), val(fasta), val(report)

    output:
    val accessioned_filename, emit: accessioned_filenames
    path "${accessioned_filename}.tmp", emit: accession_done

    script:
    def pipeline_parameters = ""
    pipeline_parameters += " --parameters.assemblyAccession=" + assembly_accession.toString()
    pipeline_parameters += " --parameters.vcfAggregation=" + aggregation.toString()
    pipeline_parameters += " --parameters.fasta=" + fasta.toString()
    pipeline_parameters += " --parameters.assemblyReportUrl=file:" + report.toString()
    pipeline_parameters += " --parameters.vcf=" + vcf_file.toString()

    accessioned_filename = vcf_filename.take(vcf_filename.indexOf(".vcf")) + ".accessioned.vcf"
    log_filename = "accessioning.${vcf_filename}"

    pipeline_parameters += " --parameters.outputVcf=" + "${params.public_dir}/${accessioned_filename}"


    """
    (java -Xmx6g -jar $params.jar.accession_pipeline --spring.config.location=file:$params.accession_job_props $pipeline_parameters) || \
    # If accessioning fails due to missing variants, but the only missing variants are structural variants,
    # then we should treat this as a success from the perspective of the automation.
    # TODO revert once accessioning pipeline properly registers structural variants
        [[ \$(grep -o 'Skipped processing structural variant' ${params.logs_dir}/${log_filename}.log | wc -l) \
           == \$(grep -oP '\\d+(?= unaccessioned variants need to be checked)' ${params.logs_dir}/${log_filename}.log) ]]
    echo "done" > ${accessioned_filename}.tmp
    """
}


/*
 * Sort and compress accessioned VCFs
 */
process sort_and_compress_vcf {
    publishDir params.public_dir,
	mode: 'copy'

    input:
    path tmp_file

    output:
    path "*.gz", emit: compressed_vcf

    """
    filename=\$(basename $tmp_file)
    filename=\${filename%.*}
    $params.executable.bcftools sort -O z -o \${filename}.gz ${params.public_dir}/\${filename}
    """
}


process csi_index_vcf {
    publishDir params.public_dir,
	mode: 'copy'

    input:
    path compressed_vcf

    output:
    path "${compressed_vcf}.csi", emit: csi_indexed_vcf

    """
    $params.executable.bcftools index -c $compressed_vcf
    """
}


/*
 * Copy files from eva_public to FTP folder.
 */
 process copy_to_ftp {
    label 'datamover'

    input:
    // ensures that all indices are done before we copy
    file csi_indices
    val accessioned_vcfs

    script:
    if( accessioned_vcfs.size() > 0 )
        """
        cd $params.public_dir
        # remove the uncompressed accessioned vcf file
        rm -f ${accessioned_vcfs.join(' ')}
        rsync -va * ${params.public_ftp_dir}/${params.project_accession}
        ls -l ${params.public_ftp_dir}/${params.project_accession}/*
        """
    else
        """
        cd $params.public_dir
        rsync -va * ${params.public_ftp_dir}/${params.project_accession}
        ls -l ${params.public_ftp_dir}/${params.project_accession}/*
        """
 }

/*
 * Load into variant db.
 */
process load_variants_vcf {
    clusterOptions {
        return "-o $params.logs_dir/load_variants.${vcf_filename}.log \
                -e $params.logs_dir/load_variants.${vcf_filename}.err"
    }

    input:
    tuple val(vcf_filename), val(vcf_file), val(fasta), val(analysis_accession), val(db_name), val(vep_version), val(vep_cache_version), val(vep_species), val(aggregation)

    output:
    val true, emit: variant_load_complete

    memory '5 GB'

    script:
    def pipeline_parameters = " --spring.batch.job.names=load-vcf-job"
    pipeline_parameters += " --input.vcf.aggregation=" + aggregation.toString().toUpperCase()
    pipeline_parameters += " --input.vcf=" + vcf_file.toRealPath().toString()
    pipeline_parameters += " --input.vcf.id=" + analysis_accession.toString()
    pipeline_parameters += " --input.fasta=" + fasta.toString()
    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.load_job_props --parameters.path=$params.load_job_props $pipeline_parameters
    """
}


/*
 * Run VEP using eva-pipeline.
 */
process run_vep_on_variants {
    clusterOptions {
        return "-o $params.logs_dir/annotation.${analysis_accession}.log \
                -e $params.logs_dir/annotation.${analysis_accession}.err"
    }

    when:
    vep_version.trim() != "" && vep_cache_version.trim() != ""

    input:
    tuple  val(fasta), val(analysis_accession), val(db_name), val(vep_version), val(vep_cache_version), val(vep_species)
    val variant_load_complete

    output:
    val true, emit: vep_run_complete

    memory '5 GB'

    script:
    def pipeline_parameters = ""

    pipeline_parameters += " --spring.batch.job.names=annotate-variants-job"
    pipeline_parameters += " --input.vcf.id=" + analysis_accession.toString()
    pipeline_parameters += " --input.fasta=" + fasta.toString()

    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()

    pipeline_parameters += " --annotation.skip=false"
    pipeline_parameters += " --app.vep.version=" + vep_version.toString()
    pipeline_parameters += " --app.vep.path=" + "${params.vep_path}/ensembl-vep-release-${vep_version}/vep"
    pipeline_parameters += " --app.vep.cache.version=" + vep_cache_version.toString()
    pipeline_parameters += " --app.vep.cache.species=" + vep_species.toString()

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.load_job_props --parameters.path=$params.load_job_props $pipeline_parameters
    """
}



/*
 * Calculate statistics using eva-pipeline.
 */
process calculate_statistics_vcf {
    clusterOptions {
        return "-o $params.logs_dir/statistics.${analysis_accession}.log \
                -e $params.logs_dir/statistics.${analysis_accession}.err"
    }

    when:
    // Statistics calculation is not required for Already aggregated analysis/study
    aggregation.toString() == "none"

    input:
    tuple val(analysis_accession), val(db_name), val(aggregation), val(vcf_files)
    val variant_load_complete

    output:
    val true, emit: statistics_calc_complete

    memory '5 GB'

    script:
    def pipeline_parameters = ""


    pipeline_parameters += " --spring.batch.job.names=calculate-statistics-job"

    pipeline_parameters += " --input.vcf.aggregation=" + aggregation.toString().toUpperCase()
    pipeline_parameters += " --input.vcf=" + file(vcf_files[0]).toRealPath().toString() // If there are multiple file only use the first
    pipeline_parameters += " --input.vcf.id=" + analysis_accession.toString()

    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.load_job_props --parameters.path=$params.load_job_props $pipeline_parameters
    """
}

/*
 * Import Accession Into Variant warehouse
 */
process import_accession {
    clusterOptions {
        log_filename = vcf_file.getFileName().toString()
        return "-o $params.logs_dir/acc_import.${log_filename}.log \
                -e $params.logs_dir/acc_import.${log_filename}.err"
    }

    input:
    tuple val(vcf_file), val(db_name)
    val all_accession_complete
    val variant_load_output

    memory '5 GB'

    script:
    def pipeline_parameters = ""

    accessioned_report_name = vcf_file.getFileName().toString().replace('.vcf','.accessioned.vcf')
    pipeline_parameters += " --input.accession.report=" + "${params.project_dir}/60_eva_public/${accessioned_report_name}"
    pipeline_parameters += " --spring.batch.job.names=accession-import-job"
    pipeline_parameters += " --spring.data.mongodb.database=" + db_name.toString()

    """
    java -Xmx4G -jar $params.jar.eva_pipeline --spring.config.location=file:$params.acc_import_job_props --parameters.path=$params.acc_import_job_props $pipeline_parameters
    """
}
