#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Load variant files into variant warehouse.

    Inputs:
            --valid_vcfs            csv file with the mappings for vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name
            --aggregation_type      whether the vcfs have genotypes or allele frequencies
            --project_accession     project accession
            --load_job_props        job-specific properties, passed as a map
            --eva_pipeline_props    main properties file for eva pipeline
            --project_dir           project directory
            --logs_dir              logs directory
    """
}

params.valid_vcfs = null
params.project_accession = null
params.load_job_props = null
params.eva_pipeline_props = null
params.project_dir = null
params.logs_dir = null
// executables
params.executable = ["bgzip": "bgzip", "bcftools": "bcftools"]
// java jars
params.jar = ["eva_pipeline": "eva_pipeline"]
// help
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test inputs
if (!params.valid_vcfs || !params.project_accession || !params.load_job_props || !params.eva_pipeline_props || !params.project_dir || !params.logs_dir) {
    if (!params.valid_vcfs) log.warn('Provide a csv file with the mappings (vcf file, assembly accession, fasta, assembly report, analysis_accession, db_name) --valid_vcfs')
    if (!params.project_accession) log.warn('Provide project accession using --project_accession')
    if (!params.load_job_props) log.warn('Provide job-specific properties using --load_job_props')
    if (!params.eva_pipeline_props) log.warn('Provide an EVA Pipeline properties file using --eva_pipeline_props')
    if (!params.project_dir) log.warn('Provide project directory using --project_dir')
    if (!params.logs_dir) log.warn('Provide logs directory using --logs_dir')
    exit 1, helpMessage()
}

/*
If the aggregation type is NONE (genotyped VCF) the csv file with the mapping between vcf file, assembly accession,
fasta, assembly report, analysis_accession, db_name will be grouped by analysis accession and the vcf files per
analysis will be counted to determine if a merge is needed. When merge is not needed a symbolic link will be created
to the input vcf file

If the aggregation type is different from none (SIMPLE = aggregated VCF, with allele frequencies) the files are not
merged and passed directly to create the properties
**/
(vcfs_to_merge, unmerged_vcfs) = (
    params.aggregation_type.toLowerCase().equals("none")
    ? [Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name)}
            .groupTuple(by:2)
            .map{row -> tuple(row[0], row[0].size(), row[1][0], row[2], row[3][0]) },
       Channel.empty()]
    : [Channel.empty(),
       Channel.fromPath(params.valid_vcfs)
            .splitCsv(header:true)
            .map{row -> tuple(file(row.vcf_file), file(row.fasta), row.analysis_accession, row.db_name)}] )


/*
 * Merge VCFs horizontally, i.e. by sample.
 */
process merge_vcfs {
    input:
    set vcf_files, file_count, fasta, analysis_accession, db_name from vcfs_to_merge
    output:
    // output will contain the absolute path to the VCF file
    tuple "${task.workDir}/${params.project_accession}_${analysis_accession}_merged.vcf.gz", fasta, analysis_accession, db_name into merged_vcf

    script:
    if (file_count > 1) {
        file_list = new File("${workflow.workDir}/all_files_${analysis_accession}.list")
        file_list.newWriter().withWriter{ w ->
            vcf_files.each { file -> w.write("$file\n")}
        }
        """
        $params.executable.bcftools merge --merge all --file-list ${workflow.workDir}/all_files_${analysis_accession}.list --threads 3 -O z -o ${params.project_accession}_${analysis_accession}_merged.vcf.gz
        """
    } else {
        single_file = vcf_files[0]
        """
        ln -sf ${single_file} ${params.project_accession}_${analysis_accession}_merged.vcf.gz
        """
    }
}


/*
 * Create properties files for load.
 */
process create_properties {
    input:
    // vcf_file contains the absolute path to the merged vcf file
    tuple vcf_file, fasta, analysis_accession, db_name from merged_vcf

    output:
    path "load_${vcf_file.getFileName()}.properties" into variant_load_props

    exec:
    props = new Properties()
    params.load_job_props.each { k, v ->
        props.setProperty(k, v.toString())
    }
    props.setProperty("input.vcf", vcf_file.toString())
    props.setProperty("input.vcf.id", analysis_accession.toString())
    props.setProperty("parameters.fasta", fasta.toString())
    props.setProperty("spring.data.mongodb.database", db_name.toString())
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
