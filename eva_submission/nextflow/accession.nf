#!/usr/bin/env nextflow

nextflow.enable.dsl=2

def helpMessage() {
    log.info"""
    Accession variant files and copy to public FTP.

    Inputs:
            --valid_vcfs            csv file with the mappings for vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name, aggregation
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
if (!params.valid_vcfs || !params.project_accession || !params.instance_id || !params.accession_job_props || !params.public_ftp_dir || !params.accessions_dir || !params.public_dir || !params.logs_dir || !params.accession_job_props.'parameters.taxonomyAccession') {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.project_accession) log.warn('Provide a project accession using --project_accession')
    if (!params.instance_id) log.warn('Provide an instance id using --instance_id')
    if (!params.accession_job_props) log.warn('Provide job-specific properties using --accession_job_props')
    if (!params.accession_job_props.'parameters.taxonomyAccession') log.warn('Provide taxonomy_id in the job-specific properties (--accession_job_props) using field taxonomyAccession')
    if (!params.public_ftp_dir) log.warn('Provide public FTP directory using --public_ftp_dir')
    if (!params.accessions_dir) log.warn('Provide accessions directory using --accessions_dir')
    if (!params.public_dir) log.warn('Provide public directory using --public_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

/*
Sequence of processes in case of:
    non-human study:
                create_properties -> accession_vcf -> sort_and_compress_vcf -> csi_index_vcf -> copy_to_ftp
    human study (skip accessioning):
                csi_index_vcf -> copy_to_ftp

process                     input channels
create_properties   ->      valid_vcfs
csi_index_vcf       ->      csi_vcfs and compressed_vcf

1. Check if the study we are working with is a human study or non-human by comparing the taxonomy_id of the study with human taxonomy_id (9606).
2. Provide values to the appropriate channels enabling them to start the corresponding processes. In case of non-human studies we want to start process
   "create_properties" while in case of human studies we want to start processes "csi_index_vcf".

non-human study:
  - Initialize valid_vcfs channel with value so that it can start the process "create_properties".
  - Initialize csi_vcfs channels as empty. This makes sure the processes "csi_index_vcf" are not started at the outset.
    These processes will only be able to start after the process "sort_and_compress_vcf" finishes and create channels compressed_vcf with values.

human study:
  - Initialize valid_vcfs channel as empty, ensuring the process "create_properties" is not started and in turn accessioning part is also skipped,  as the process
    "accession_vcf" depends on the output channels created by the process create_properties.
  - Initialize csi_vcfs with values enabling them to start the processes "csi_index_vcf".
*/
workflow {
    is_human_study = (params.accession_job_props.'parameters.taxonomyAccession' == 9606)

    if (is_human_study) {
        csi_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file))}
        accessioned_files_to_rm = Channel.empty()
    } else {
        valid_vcfs = Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), row.assembly_accession, row.aggregation, file(row.fasta), file(row.report))}
        create_properties(valid_vcfs)
        accession_vcf(create_properties.out.accession_props, create_properties.out.accessioned_filenames, create_properties.out.log_filenames)
        sort_and_compress_vcf(accession_vcf.out.accession_done)
        csi_vcfs = sort_and_compress_vcf.out.compressed_vcf
        accessioned_files_to_rm = create_properties.out.accessioned_filenames
    }
    csi_index_vcf(csi_vcfs)
    copy_to_ftp(csi_index_vcf.out.csi_indexed_vcf.toList(), accessioned_files_to_rm.toList())
}

/*
 * Create properties files for accession.
 */
process create_properties {
    input:
    tuple path(vcf_file), val(assembly_accession), val(aggregation), path(fasta), path(report)

    output:
    path "${vcf_file.getFileName()}_accessioning.properties", emit: accession_props
    val accessioned_filename, emit: accessioned_filenames
    val log_filename, emit: log_filenames

    exec:
    props = new Properties()
    params.accession_job_props.each { k, v ->
        props.setProperty(k, v.toString())
    }
    props.setProperty("parameters.assemblyAccession", assembly_accession.toString())
    props.setProperty("parameters.vcfAggregation", aggregation.toString())
    props.setProperty("parameters.fasta", fasta.toString())
    props.setProperty("parameters.assemblyReportUrl", "file:" + report.toString())
    props.setProperty("parameters.vcf", vcf_file.toString())
    vcf_filename = vcf_file.getFileName().toString()
    accessioned_filename = vcf_filename.take(vcf_filename.indexOf(".vcf")) + ".accessioned.vcf"
    log_filename = "accessioning.${vcf_filename}"
    props.setProperty("parameters.outputVcf", "${params.public_dir}/${accessioned_filename}")

    // need to explicitly store in workDir so next process can pick it up
    // see https://github.com/nextflow-io/nextflow/issues/942#issuecomment-441536175
    props_file = new File("${task.workDir}/${vcf_filename}_accessioning.properties")
    props_file.createNewFile()
    props_file.newWriter().withWriter { w ->
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

    memory '6.7 GB'

    input:
    path accession_properties
    val accessioned_filename
    val log_filename

    output:
    path "${accessioned_filename}.tmp", emit: accession_done

    """
    filename=\$(basename $accession_properties)
    filename=\${filename%.*}
    (java -Xmx6g -jar $params.jar.accession_pipeline --spring.config.name=\$filename) || \
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
