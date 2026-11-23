import json
import os
from collections import defaultdict
from copy import deepcopy

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission import __version__
from eva_submission.biosample_submission.biosamples_submitters import SampleMetadataSubmitter
from eva_submission.biosample_submission.sample_record import SampleRecord, records_to_dicts
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


logger = log_cfg.get_logger(__name__)


def needs_sample_records_upgrade(submission_cfg):
    """Configs brokered before the sample records were introduced only have a sample name to accession dict."""
    return bool(submission_cfg.query('brokering', 'Biosamples', 'Samples')) and \
        submission_cfg.query('brokering', 'Biosamples', 'sample_records') is None


def upgrade_samples_to_sample_records(submission_cfg, samples_in_evapro_analysis=None):
    """
    Replaces the legacy sample name to accession dict (brokering > Biosamples > Samples) with sample records.
    The records are rebuilt from, in order of preference:
     - EVAPRO, when every analysis of the submission has samples loaded,
     - the metadata JSON or spreadsheet, when every sample in it can be given an accession,
     - the legacy dict itself, assigning each sample to every analysis of the submission.
    samples_in_evapro_analysis is a function that returns the (BioSample accession, name in file) of the samples of
    an analysis accession.
    """
    if not needs_sample_records_upgrade(submission_cfg):
        return
    sample_name_to_accession = submission_cfg.query('brokering', 'Biosamples', 'Samples')
    sample_records = (
        _sample_records_from_evapro(submission_cfg, samples_in_evapro_analysis)
        or _sample_records_from_metadata(submission_cfg, sample_name_to_accession)
        or _sample_records_from_sample_map(submission_cfg, sample_name_to_accession)
    )
    submission_cfg.set('brokering', 'Biosamples', 'sample_records', value=records_to_dicts(sample_records))
    submission_cfg.pop('brokering', 'Biosamples', 'Samples')


def _sample_records_from_evapro(submission_cfg, samples_in_evapro_analysis):
    analysis_alias_to_accession = submission_cfg.query('brokering', 'ena', 'ANALYSIS')
    if not samples_in_evapro_analysis or not isinstance(analysis_alias_to_accession, dict):
        return None
    analysis_aliases_per_sample = defaultdict(list)
    for analysis_alias, analysis_accession in analysis_alias_to_accession.items():
        try:
            samples = samples_in_evapro_analysis(analysis_accession)
        except Exception as e:
            logger.warning(f'Could not retrieve the samples of {analysis_accession} from EVAPRO: {e}')
            return None
        if not samples:
            logger.info(f'Analysis {analysis_accession} has no samples in EVAPRO')
            return None
        for accession, name_in_file in samples:
            analysis_aliases_per_sample[(accession, name_in_file)].append(analysis_alias)
    logger.info('Sample records rebuilt from EVAPRO')
    return [
        # Allele frequency samples without name were loaded with their accession as name in file
        SampleRecord(accession, analysis_aliases, sample_in_vcf=name_in_file if name_in_file != accession else None)
        for (accession, name_in_file), analysis_aliases in analysis_aliases_per_sample.items()
    ]


def _sample_records_from_metadata(submission_cfg, sample_name_to_accession):
    metadata_json = submission_cfg.query('validation', 'valid', 'metadata_json')
    metadata_spreadsheet = submission_cfg.query('validation', 'valid', 'metadata_spreadsheet') or \
        submission_cfg.query('submission', 'metadata_spreadsheet')
    if metadata_json and os.path.exists(metadata_json):
        with open(metadata_json) as open_file:
            samples = [
                (sample.get('bioSampleAccession'), sample.get('sampleInVCF'), sample.get('analysisAlias') or [])
                for sample in json.load(open_file).get('sample', [])
            ]
    elif metadata_spreadsheet and os.path.exists(metadata_spreadsheet):
        samples = [
            (row.get('Sample Accession'), SampleMetadataSubmitter._sample_name_in_vcf(row),
             SampleMetadataSubmitter._analysis_aliases(row))
            for row in EvaXlsxReader(metadata_spreadsheet).samples
        ]
    else:
        return None
    sample_records = []
    for accession_in_metadata, sample_in_vcf, analysis_aliases in samples:
        # Same precedence as the legacy brokering: the brokered accession first, then the one from the metadata
        accession = sample_name_to_accession.get(sample_in_vcf) or accession_in_metadata
        if not accession:
            logger.warning(f'Sample {sample_in_vcf} from the metadata has no accession in the config')
            return None
        if isinstance(analysis_aliases, str):
            analysis_aliases = [analysis_aliases]
        sample_records.append(SampleRecord(accession, analysis_aliases, sample_in_vcf=sample_in_vcf))
    logger.info('Sample records rebuilt from the metadata')
    return sample_records


def _sample_records_from_sample_map(submission_cfg, sample_name_to_accession):
    analysis_aliases = list(submission_cfg.query('brokering', 'analyses', ret_default={}))
    logger.warning('Sample records could not be rebuilt from EVAPRO or the metadata: '
                   f'each sample is assigned to all the analyses {analysis_aliases}')
    return [
        SampleRecord(accession, analysis_aliases, sample_in_vcf=sample_name)
        for sample_name, accession in sample_name_to_accession.items()
    ]


def upgrade_version_1_15_to_1_16(submission_cfg, submission_dir):
    """
    Upgrades a version 1.15 directory structure to version 1.16 to change the path to ingestion nextflow directories
    """
    project_dir = submission_cfg.query('ingestion', 'project_dir')
    if not project_dir:
        # This Submission never went through ingestion so there is no need to change anything
        return

    for sub_dir_or_file in os.listdir(project_dir):
        source = os.path.join(project_dir, sub_dir_or_file)
        dest = os.path.join(submission_dir, sub_dir_or_file)
        if os.path.exists(dest):
            if (os.path.islink(dest) and os.readlink(dest) == source) or dest == source:
                logger.debug(f'symbolic link {dest} already exist and is correct')
            else:
                if os.path.islink(dest):
                    raise ValueError(f'Attempting to create a link from {source} to {dest} but {dest} already exist and it points to {os.readlink(dest)}')
                else:
                    raise ValueError(f'Attempting to create a link from {source} to {dest} but {dest} already exist and it is not a simlink')
        else:
            os.symlink(source, dest)


def upgrade_version_1_14_to_1_15(submission_cfg):
    """
    Upgrades a version 1.14 config to version 1.15 to change the path to ingestion nextflow directories
    """
    accession_nexflow_dir = submission_cfg.query('ingestion', 'accession', 'nextflow_dir')
    variant_load_nexflow_dir = submission_cfg.query('ingestion', 'variant_load', 'nextflow_dir')
    if accession_nexflow_dir:
        submission_cfg.set('ingestion', 'accession_and_load', 'nextflow_dir', 'accession', value=accession_nexflow_dir)
    if variant_load_nexflow_dir:
        submission_cfg.set('ingestion', 'accession_and_load', 'nextflow_dir', 'variant_load', value=variant_load_nexflow_dir)

    # Set version once we've successfully upgraded
    submission_cfg.set('version', value=__version__)


def upgrade_version_0_1(submission_cfg, analysis_alias=None):
    """
    Upgrades a version 0.x config to version 1, using the provided analysis alias for all files.
    """
    if 'submission' not in submission_cfg:
        logger.error('Need submission config section to upgrade')
        logger.error('Try running prepare_submission or prepare_backlog_study to build a config from scratch.')
        raise ValueError('Need submission config section to upgrade')

    # Note: if we're converting an old config, there's only one analysis
    if not analysis_alias:
        analysis_alias = get_analysis_alias_from_metadata(submission_cfg)
    analysis_data = {
        'assembly_accession': submission_cfg.pop('submission', 'assembly_accession'),
        'assembly_fasta': submission_cfg.pop('submission', 'assembly_fasta'),
        'assembly_report': submission_cfg.pop('submission', 'assembly_report'),
        'vcf_files': submission_cfg.pop('submission', 'vcf_files')
    }
    analysis_dict = {analysis_alias: analysis_data}
    submission_cfg.set('submission', 'analyses', value=analysis_dict)

    if 'validation' in submission_cfg:
        submission_cfg.pop('validation', 'valid', 'vcf_files')
        submission_cfg.set('validation', 'valid', 'analyses', value=analysis_dict)

    if 'brokering' in submission_cfg:
        brokering_vcfs = {
            vcf_file: index_dict
            for vcf_file, index_dict in submission_cfg.pop('brokering', 'vcf_files').items()
        }
        brokering_analyses = deepcopy(analysis_dict)
        brokering_analyses[analysis_alias]['vcf_files'] = brokering_vcfs
        submission_cfg.set('brokering', 'analyses', value=brokering_analyses)
        analysis_accession = submission_cfg.pop('brokering', 'ena', 'ANALYSIS')
        submission_cfg.set('brokering', 'ena', 'ANALYSIS', analysis_alias, value=analysis_accession)

    # Set version once we've successfully upgraded
    submission_cfg.set('version', value=__version__)


def get_analysis_alias_from_metadata(submission_cfg):
    """
    Returns analysis alias only if we find a metadata spreadsheet and it has exactly one analysis.
    Otherwise provides an error message and raise an error.
    """
    metadata_spreadsheet = submission_cfg.query('submission', 'metadata_spreadsheet')
    if metadata_spreadsheet:
        reader = EvaXlsxReader(metadata_spreadsheet)
        if len(reader.analysis) == 1:
            return reader.analysis[0].get('Analysis Alias')

        if len(reader.analysis) > 1:
            logger.error("Can't assign analysis alias: multiple analyses found in metadata!")
        else:
            logger.error("Can't assign analysis alias: no analyses found in metadata!")
    else:
        logger.error("Can't assign analysis alias: no metadata found!")
    logger.error("Try running upgrade_config and passing an analysis alias explicitly.")
    raise ValueError("Can't find an analysis alias for config upgrade.")
