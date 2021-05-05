#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Accession variant files and copy to public FTP.

    Inputs:
            --valid_vcfs            valid vcfs to load
            --project_accession     project accession
            --instance_id           instance id to run accessioning
            --accession_job_props   job-specific properties, passed as a map
            --public_ftp_dir        public FTP directory
            --accessions_dir        accessions directory (for properties files)
            --public_dir            directory for files to be made public
            --logs_dir              logs directory
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
// executables
params.executable = ["bcftools": "bcftools", "tabix": "tabix"]
// java jars
params.jar = ["accession_pipeline": "accession_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.valid_vcfs || !params.project_accession || !params.instance_id || !params.accession_job_props || !params.public_ftp_dir || !params.accessions_dir || !params.public_dir || !params.logs_dir || !params.accession_job_props.taxonomy_id) {
    if (!params.valid_vcfs) log.warn('Provide validated vcfs using --valid_vcfs')
    if (!params.project_accession) log.warn('Provide a project accession using --project_accession')
    if (!params.instance_id) log.warn('Provide an instance id using --instance_id')
    if (!params.accession_job_props) log.warn('Provide job-specific properties using --accession_job_props')
    if (!params.accession_job_props.taxonomy_id) log.warn('Provide taxonomy_id in the job-specific properties (--accession_job_props) using field taxonomy_id')
    if (!params.public_ftp_dir) log.warn('Provide public FTP directory using --public_ftp_dir')
    if (!params.accessions_dir) log.warn('Provide accessions directory using --accessions_dir')
    if (!params.public_dir) log.warn('Provide public directory using --public_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

/*
Sequence of processes in case of:
    non-human study:
                create_properties -> accession_vcf -> sort_and_compress_vcf -> tabix_index_vcf and csi_index_vcf -> copy_to_ftp
    human study (skip accessioning):
                tabix_index_vcf and csi_index_vcf -> copy_to_ftp

process                     input channels
create_properties   ->      valid_vcfs
tabix_index_vcf     ->      tabix_vcfs and compressed_vcf1
csi_index_vcf       ->      csi_vcfs and compressed_vcf2

1. Check if the study we are working with is a human study or non-human by comparing the taxonomy_id of the study with human taxonomy_id (9606).
2. Provide values to the appropriate channels enabling them to start the corresponding processes. In case of non-human studies we want to start process
   "create_properties" while in case of human studies we want to start processes "tabix_index_vcf" and "csi_index_vcf".

non-human study:
  - Initialize valid_vcfs channel with value so that it can start the process "create_properties".
  - Initialize tabix_vcfs and csi_vcfs channels as empty. This makes sure the processes "tabix_index_vcf" and "csi_index_vcf" are not started at the outset.
    These processes will only be able to start after the process "sort_and_compress_vcf" finishes and create channels compresses_vcf1 and compressed_vcf2 with values.

human study:
  - Initialize valid_vcfs channel as empty, ensuring the process "create_properties" is not started and in turn accessioning part is also skipped,  as the process
    "accession_vcf" depends on the output channels created by the process create_properties.
  - Initialize tabix_vcfs and csi_vcfs with values enabling them to start the processes "tabix_index_vcf" and "csi_index_vcf".
*/
is_human_study = (params.accession_job_props.taxonomy_id == 9606)
(valid_vcfs, tabix_vcfs, csi_vcfs) = ( is_human_study
                     ? [ Channel.empty(), Channel.fromPath(params.valid_vcfs), Channel.fromPath(params.valid_vcfs) ]
                     : [ Channel.fromPath(params.valid_vcfs), Channel.empty(), Channel.empty() ] )

/*
 * Create properties files for accession.
 */
process create_properties {
    input:
    val vcf_file from valid_vcfs

    output:
    path "${vcf_file.getFileName()}_accessioning.properties" into accession_props
    val accessioned_filename into accessioned_filenames
    val log_filename into log_filenames

    exec:
    props = new Properties()
    params.accession_job_props.each { k, v ->
        props.setProperty(k, v.toString())
    }
    props.setProperty("parameters.vcf", vcf_file.toString())
    vcf_filename = vcf_file.getFileName().toString()
    accessioned_filename = vcf_filename.take(vcf_filename.indexOf(".vcf")) + ".accessioned.vcf"
    log_filename = "accessioning.${vcf_filename}"
    props.setProperty("parameters.outputVcf", "${params.public_dir}/${accessioned_filename}")

    // need to explicitly store in workDir so next process can pick it up
    // see https://github.com/nextflow-io/nextflow/issues/942#issuecomment-441536175
    props_file = new File("${task.workDir}/${vcf_filename}_accessioning.properties")
    props_file.createNewFile()
    props_file.withWriter { w ->
        props.each { k, v ->
            w.write("$k=$v\n")
        }
    }
    // make a copy for debugging purposes
    new File("${params.accessions_dir}/${vcf_filename}_accessioning.properties") << props_file.asWritable()
}


/*
 * Accession VCFs
 */
process accession_vcf {
    clusterOptions "-g /accession/instance-${params.instance_id} \
                    -o $params.logs_dir/${log_filename}.log \
                    -e $params.logs_dir/${log_filename}.err"

    memory '8 GB'

    input:
    path accession_properties from accession_props
    val accessioned_filename from accessioned_filenames
    val log_filename from log_filenames

    output:
    path "${accessioned_filename}.tmp" into accession_done

    """
    filename=\$(basename $accession_properties)
    filename=\${filename%.*}
    (java -Xmx7g -jar $params.jar.accession_pipeline --spring.config.name=\$filename) || \
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
    path tmp_file from accession_done

    output:
    // used by both tabix and csi indexing processes
    path "*.gz" into compressed_vcf1, compressed_vcf2

    """
    filename=\$(basename $tmp_file)
    filename=\${filename%.*}
    $params.executable.bcftools sort -O z -o \${filename}.gz ${params.public_dir}/\${filename}
    """
}


/*
 * Index the compressed VCF file
 */
process tabix_index_vcf {
    publishDir params.public_dir,
	mode: 'copy'

    input:
    path compressed_vcf from tabix_vcfs.mix(compressed_vcf1)

    output:
    path "${compressed_vcf}.tbi" into tbi_indexed_vcf

    """
    $params.executable.tabix -p vcf $compressed_vcf
    """
}


process csi_index_vcf {
    publishDir params.public_dir,
	mode: 'copy'

    input:
    path compressed_vcf from csi_vcfs.mix(compressed_vcf2)

    output:
    path "${compressed_vcf}.csi" into csi_indexed_vcf

    """
    $params.executable.bcftools index -c $compressed_vcf
    """
}


/*
 * Copy files from eva_public to FTP folder.
 */
 process copy_to_ftp {
    input:
    // ensures that all indices are done before we copy
    file csi_indices from csi_indexed_vcf.toList()
    file tbi_indices from tbi_indexed_vcf.toList()

    """
    cd $params.public_dir
    rsync -va * ${params.public_ftp_dir}/${params.project_accession}
    ls -l ${params.public_ftp_dir}/${params.project_accession}/*
    """
 }
