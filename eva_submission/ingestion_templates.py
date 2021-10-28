from ebi_eva_common_pyutils.config import cfg


# Name of collections in variant warehouse
annotation_metadata_collection_name = 'annotationMetadata_2_0'
annotation_collection_name = 'annotation_2_0'


def accession_props_template(
        instance_id,
        taxonomy_id,
        project_accession,
        postgres_url,
        postgres_user,
        postgres_pass,
        mongo_host,
        mongo_user,
        mongo_pass,
):
    """
    Get all properties needed for this accessioning job, except for the input
    and output filenames which are filled in by Nextflow.
    """
    return {
        'accessioning.instanceId': f'instance-{instance_id}',
        'accessioning.submitted.categoryId': 'ss',
        'accessioning.monotonic.ss.blockSize': 100000,
        'accessioning.monotonic.ss.blockStartValue': 5000000000,
        'accessioning.monotonic.ss.nextBlockInterval': 1000000000,
        'parameters.taxonomyAccession': taxonomy_id,
        'parameters.projectAccession': project_accession,
        'parameters.chunkSize': 100,
        'parameters.forceRestart': False,
        'parameters.contigNaming': 'NO_REPLACEMENT',
        'spring.batch.job.names': 'CREATE_SUBSNP_ACCESSION_JOB',
        'spring.datasource.driver-class-name': 'org.postgresql.Driver',
        'spring.datasource.url': postgres_url,
        'spring.datasource.username': postgres_user,
        'spring.datasource.password': postgres_pass,
        'spring.datasource.tomcat.max-active': 3,
        'spring.jpa.generate-ddl': True,
        'spring.data.mongodb.host': mongo_host,
        'spring.data.mongodb.port': 27017,
        'spring.data.mongodb.database': 'eva_accession_sharded',
        'spring.data.mongodb.username': mongo_user,
        'spring.data.mongodb.password': mongo_pass,
        'spring.data.mongodb.authentication-database': 'admin',
        'mongodb.read-preference': 'secondaryPreferred',
        'spring.main.web-environment': False,
        'spring.main.allow-bean-definition-overriding': True,
        'spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation': True,
    }


def variant_load_props_template(
        project_accession,
        study_name,
        output_dir,
        annotation_dir,
        stats_dir
):
    """
    Get all properties needed for this variant load job, except for the vcf file
    which is filled in by Nextflow after (optional) merge.
    """
    return {
        'input.study.id': project_accession,
        'input.study.name': study_name,
        'input.study.type': 'COLLECTION',
        'output.dir': str(output_dir),
        'output.dir.annotation': str(annotation_dir),
        'output.dir.statistics': str(stats_dir),
        'db.collections.files.name': 'files_2_0',
        'db.collections.variants.name': 'variants_2_0',
        'db.collections.annotation-metadata.name': annotation_metadata_collection_name,
        'db.collections.annotations.name': annotation_collection_name,
        'app.vep.cache.path': cfg['vep_cache_path'],
        'app.vep.num-forks': 4,
        'app.vep.timeout': 500,
        'statistics.skip': False,
        'annotation.overwrite': False,
        'config.chunk.size': 200,
        'spring.jpa.properties.hibernate.jdbc.lob.non_contextual_creation': True,
    }
