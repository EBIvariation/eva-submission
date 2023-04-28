#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Accession variant files and copy to public FTP.

    Inputs:
            --valid_vcfs            csv file with the mappings for vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name, aggregation
            --project_accession     project accession
            --instance_id           instance id to run accessioning
            --accession_job_props   properties file for accessioning job
            --public_ftp_dir        public FTP directory
            --accessions_dir        accessions directory (for properties files)
            --public_dir            directory for files to be made public
            --logs_dir              logs directory
            --taxonomy              taxonomy id
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
// executables
params.executable = ["bcftools": "bcftools", "tabix": "tabix"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.valid_vcfs || !params.project_accession || !params.instance_id || !params.accession_job_props || !params.public_ftp_dir || !params.accessions_dir || !params.public_dir || !params.logs_dir || !params.taxonomy) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.project_accession) log.warn('Provide a project accession using --project_accession')
    if (!params.instance_id) log.warn('Provide an instance id using --instance_id')
    if (!params.accession_job_props) log.warn('Provide job-specific properties using --accession_job_props')
    if (!params.taxonomy) log.warn('Provide taxonomy id using --taxonomy')
    if (!params.public_ftp_dir) log.warn('Provide public FTP directory using --public_ftp_dir')
    if (!params.accessions_dir) log.warn('Provide accessions directory using --accessions_dir')
    if (!params.public_dir) log.warn('Provide public directory using --public_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

/*
Sequence of processes in case of:
    non-human study:
                accession_vcf -> sort_and_compress_vcf -> csi_index_vcf -> copy_to_ftp
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
    // used by csi indexing process
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
        rm ${accessioned_vcfs.join(' ')}
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
