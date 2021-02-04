
def variant_load_properties(
        project_accession,
        analysis_accession,
        vcf_path,
        study_name,
        assembly_path,
        db_name,
        species,
):
    # TODO VEP stuff, among other things
    return f"""
# JOB
spring.batch.job.names=genotyped-vcf-job

# SUBMISSION FIELDS
input.study.id={project_accession}
input.vcf.id={analysis_accession}
input.vcf={vcf_path}
input.vcf.aggregation=NONE

input.study.name={study_name}
input.study.type=COLLECTION

input.pedigree=
input.fasta={assembly_path}

output.dir=/nfs/production3/eva/data/{project_accession}/40_transformed/
output.dir.annotation=/nfs/production3/eva/data/{project_accession}/51_annotation/
output.dir.statistics=/nfs/production3/eva/data/{project_accession}/50_stats/


# MONGODB (MongoProperties)
spring.data.mongodb.database={db_name}

db.collections.files.name=files_2_0
db.collections.variants.name=variants_2_0
db.collections.annotation-metadata.name=annotationMetadata_2_0
db.collections.annotations.name=annotations_2_0

# External applications
## VEP
app.vep.version=82
app.vep.path=/nfs/production3/eva/software/vep/ensembl-tools-release-82/scripts/variant_effect_predictor/variant_effect_predictor.pl
app.vep.cache.version=82
app.vep.cache.path=/nfs/production3/eva/databases/vep-cache/
app.vep.cache.species={species}
app.vep.num-forks=4
app.vep.timeout=500


# STEPS MANAGEMENT
## Skip steps
statistics.skip=false
annotation.skip=false
annotation.overwrite=false

config.chunk.size=200
"""
