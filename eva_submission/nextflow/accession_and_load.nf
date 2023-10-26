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
    is_human_study = (params.taxonomy == 9606)
    if (is_human_study) {
        csi_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file))}
        accessioned_files_to_rm = Channel.empty()
    } else {
        valid_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), row.assembly_accession, row.aggregation, file(row.fasta), file(row.report))}
        accession_vcf(valid_vcfs)
        sort_and_compress_vcf(accession_vcf.out.accession_done)
        csi_vcfs = sort_and_compress_vcf.out.compressed_vcf
        accessioned_files_to_rm = accession_vcf.out.accessioned_filenames
    }
    csi_index_vcf(csi_vcfs)
    copy_to_ftp(csi_index_vcf.out.csi_indexed_vcf.toList(), accessioned_files_to_rm.toList())

    annotated_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name, row.vep_version, row.vep_cache_version, row.vep_species, row.aggregation)}
    load_vcf(annotated_vcfs)

    if (!is_human_study) {
        vcf_files_dbname = Channel.fromPath(params.valid_vcfs)
                .splitCsv(header:true)
                .map{row -> tuple(file(row.vcf_file), row.db_name)}
        // the vcf_files_dbname give the link between input file and compressed_vcf is to ensure the accessioning has
        // been completed
        import_accession(vcf_files_dbname, sort_and_compress_vcf.out.compressed_vcf, load_vcf.out.variant_load_complete)
    }
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
    tuple val(vcf_file), val(assembly_accession), val(aggregation), val(fasta), val(report)

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

    vcf_filename = vcf_file.getFileName().toString()
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

    if (vep_version.trim() == "" || vep_cache_version.trim() == "") {
        pipeline_parameters += " --annotation.skip=true"
    } else {
        pipeline_parameters += " --annotation.skip=false"
        pipeline_parameters += " --app.vep.version=" + vep_version.toString()
        pipeline_parameters += " --app.vep.path=" + "${params.vep_path}/ensembl-vep-release-${vep_version}/vep"
        pipeline_parameters += " --app.vep.cache.version=" + vep_cache_version.toString()
        pipeline_parameters += " --app.vep.cache.species=" + vep_species.toString()
    }

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
    path compressed_vcf
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
