from ebi_eva_common_pyutils.config import cfg


def accession_props_template(
        instance_id,
        assembly_accession,
        taxonomy_id,
        project_accession,
        vcf_path,
        aggregation,
        fasta,
        output_vcf,
        report,
        postgres_url,
        postgres_user,
        postgres_pass,
        mongo_host,
        mongo_user,
        mongo_pass,
):
    return f"""
accessioning.instanceId=instance-{instance_id}
accessioning.submitted.categoryId=ss
accessioning.monotonic.ss.blockSize=100000
accessioning.monotonic.ss.blockStartValue=5000000000
accessioning.monotonic.ss.nextBlockInterval=1000000000

parameters.assemblyAccession={assembly_accession}
parameters.taxonomyAccession={taxonomy_id}
parameters.projectAccession={project_accession}
parameters.chunkSize=100
parameters.vcf={vcf_path}
parameters.vcfAggregation={aggregation}
parameters.forceRestart=false
parameters.fasta={fasta}
parameters.outputVcf={output_vcf}
parameters.assemblyReportUrl=file:{report}
# contigNaming available values: SEQUENCE_NAME, ASSIGNED_MOLECULE, INSDC, REFSEQ, UCSC, NO_REPLACEMENT
parameters.contigNaming=NO_REPLACEMENT

spring.batch.job.names=CREATE_SUBSNP_ACCESSION_JOB

spring.datasource.driver-class-name=org.postgresql.Driver
spring.datasource.url={postgres_url}
spring.datasource.username={postgres_user}
spring.datasource.password={postgres_pass}
spring.datasource.tomcat.max-active=3
spring.jpa.generate-ddl=true

spring.data.mongodb.host={mongo_host}
spring.data.mongodb.port=27017
spring.data.mongodb.database=eva_accession_sharded
spring.data.mongodb.username={mongo_user}
spring.data.mongodb.password={mongo_pass}
spring.data.mongodb.authentication-database=admin
mongodb.read-preference=primaryPreferred

spring.main.web-environment=false
"""


def variant_load_props_template(
        project_accession,
        analysis_accession,
        vcf_path,
        aggregation,
        study_name,
        fasta,
        output_dir,
        annotation_dir,
        stats_dir,
        db_name,
        vep_species,
        vep_version,
        vep_cache_version,
        annotation_skip=False,
):
    return f"""
# JOB
spring.batch.job.names={'genotyped-vcf-job' if aggregation == 'none' else 'aggregated-vcf-job'}

# SUBMISSION FIELDS
input.study.id={project_accession}
input.vcf.id={analysis_accession}
input.vcf={vcf_path}
input.vcf.aggregation={aggregation}

input.study.name={study_name}
input.study.type=COLLECTION

input.pedigree=
input.fasta={fasta}

output.dir={output_dir}
output.dir.annotation={annotation_dir}
output.dir.statistics={stats_dir}


# MONGODB (MongoProperties)
spring.data.mongodb.database={db_name}

db.collections.files.name=files_2_0
db.collections.variants.name=variants_2_0
db.collections.annotation-metadata.name=annotationMetadata_2_0
db.collections.annotations.name=annotations_2_0

# External applications
## VEP
app.vep.version={vep_version}
app.vep.path={cfg['vep_path']}/ensembl-vep-release-{vep_version}/vep
app.vep.cache.version={vep_cache_version}
app.vep.cache.path={cfg['vep_cache_path']}
app.vep.cache.species={vep_species}
app.vep.num-forks=4
app.vep.timeout=500


# STEPS MANAGEMENT
## Skip steps
statistics.skip=false
annotation.skip={annotation_skip}
annotation.overwrite=false

config.chunk.size=200
"""
