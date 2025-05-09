from functools import lru_cache

from ebi_eva_common_pyutils.logger import logging_config


logger = logging_config.get_logger(__name__)

job_launched_and_completed_text_map = {
    'accession': (
        {'Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] launched'},
        {'Job: [SimpleJob: [name=CREATE_SUBSNP_ACCESSION_JOB]] completed'}
    ),
    'variant_load': (
        {'Job: [FlowJob: [name=genotyped-vcf-job]] launched',
         'Job: [FlowJob: [name=aggregated-vcf-job]] launched'},
        {'Job: [FlowJob: [name=genotyped-vcf-job]] completed',
         'Job: [FlowJob: [name=aggregated-vcf-job]] completed'}
    ),
    'load_vcf': (
        {'Job: [FlowJob: [name=load-vcf-job]] launched'},
        {'Job: [FlowJob: [name=load-vcf-job]] completed'}
    ),
    'annotate_variants': (
        {'Job: [FlowJob: [name=annotate-variants-job]] launched'},
        {'Job: [FlowJob: [name=annotate-variants-job]] completed'}
    ),
    'calculate_statistics': (
        {'Job: [FlowJob: [name=calculate-statistics-job]] launched'},
        {'Job: [FlowJob: [name=calculate-statistics-job]] completed'}
    ),
    'variant-stats': (
        {'Job: [SimpleJob: [name=variant-stats-job]] launched'},
        {'Job: [SimpleJob: [name=variant-stats-job]] completed'}
    ),
    'file-stats': (
        {'Job: [SimpleJob: [name=file-stats-job]] launched'},
        {'Job: [SimpleJob: [name=file-stats-job]] completed'}
    ),
    'acc_import': (
        {'Job: [SimpleJob: [name=accession-import-job]] launched'},
        {'Job: [SimpleJob: [name=accession-import-job]] completed'}
    ),
    'clustering': (
        {'Job: [SimpleJob: [name=STUDY_CLUSTERING_JOB]] launched'},
        {'Job: [SimpleJob: [name=STUDY_CLUSTERING_JOB]] completed'}
    ),
    'clustering_qc': (
        {'Job: [SimpleJob: [name=NEW_CLUSTERED_VARIANTS_QC_JOB]] launched'},
        {'Job: [SimpleJob: [name=NEW_CLUSTERED_VARIANTS_QC_JOB]] completed'}
    ),
    'vcf_extractor': (
        {'Job: [SimpleJob: [name=EXPORT_SUBMITTED_VARIANTS_JOB]] launched'},
        {'Job: [SimpleJob: [name=EXPORT_SUBMITTED_VARIANTS_JOB]] completed'}
    ),
    'remapping_ingestion': (
        {'Job: [SimpleJob: [name=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB]] launched'},
        {'Job: [SimpleJob: [name=INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB]] completed'}
    ),
    'backpropagation': (
        {'Job: [SimpleJob: [name=BACK_PROPAGATE_NEW_RS_JOB]] launched'},
        {'Job: [SimpleJob: [name=BACK_PROPAGATE_NEW_RS_JOB]] completed'}
    )
}


@lru_cache(maxsize=None)
def did_job_complete_successfully_from_log(file_path, job_type):
    with open(file_path, 'r') as f:
        job_status = 'FAILED'
        job_launched_str, job_completed_str = job_launched_and_completed_text_map[job_type]
        for line in f:
            if any(text in line for text in job_launched_str):
                job_status = ""
            if any(text in line for text in job_completed_str):
                job_status = line.split(" ")[-1].replace("[", "").replace("]", "").strip()
        if job_status == 'COMPLETED':
            return True
        elif job_status == 'FAILED':
            return False
        else:
            logger.error(f'Could not determine status of {job_type} job in file {file_path}')
            return False


def get_failed_job_or_step_name(file_name):
    with open(file_name, 'r') as f:
        job_name = 'job name could not be retrieved'
        for line in f:
            if 'Encountered an error executing step' in line:
                job_name = line[line.index("Encountered an error executing step"): line.rindex("in job")] \
                    .strip().split(" ")[-1]

        return job_name
