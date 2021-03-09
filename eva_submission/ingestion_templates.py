
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
