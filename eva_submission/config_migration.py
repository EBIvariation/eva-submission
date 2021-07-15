from copy import deepcopy

from ebi_eva_common_pyutils.logger import logging_config as log_cfg

from eva_submission import __version__
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


logger = log_cfg.get_logger(__name__)


def upgrade_version_0_1(eload_cfg, analysis_alias=None):
    """
    Upgrades a version 0.x config to version 1, using the provided analysis alias for all files.
    """
    if 'submission' not in eload_cfg:
        logger.error('Need submission config section to upgrade')
        logger.error('Try running prepare_submission or prepare_backlog_study to build a config from scratch.')
        raise ValueError('Need submission config section to upgrade')

    # Note: if we're converting an old config, there's only one analysis
    if not analysis_alias:
        analysis_alias = get_analysis_alias_from_metadata(eload_cfg)
    analysis_data = {
        'assembly_accession': eload_cfg.pop('submission', 'assembly_accession'),
        'assembly_fasta': eload_cfg.pop('submission', 'assembly_fasta'),
        'assembly_report': eload_cfg.pop('submission', 'assembly_report'),
        'vcf_files': eload_cfg.pop('submission', 'vcf_files')
    }
    analysis_dict = {analysis_alias: analysis_data}
    eload_cfg.set('submission', 'analyses', value=analysis_dict)

    if 'validation' in eload_cfg:
        eload_cfg.pop('validation', 'valid', 'vcf_files')
        eload_cfg.set('validation', 'valid', 'analyses', value=analysis_dict)

    if 'brokering' in eload_cfg:
        brokering_vcfs = {
            vcf_file: index_dict
            for vcf_file, index_dict in eload_cfg.pop('brokering', 'vcf_files').items()
        }
        brokering_analyses = deepcopy(analysis_dict)
        brokering_analyses[analysis_alias]['vcf_files'] = brokering_vcfs
        eload_cfg.set('brokering', 'analyses', value=brokering_analyses)
        analysis_accession = eload_cfg.pop('brokering', 'ena', 'ANALYSIS')
        eload_cfg.set('brokering', 'ena', 'ANALYSIS', analysis_alias, value=analysis_accession)

    # Set version once we've successfully upgraded
    eload_cfg.set('version', value=__version__)


def get_analysis_alias_from_metadata(eload_cfg):
    """
    Returns analysis alias only if we find a metadata spreadsheet and it has exactly one analysis.
    Otherwise provides an error message and raise an error.
    """
    metadata_spreadsheet = eload_cfg.query('submission', 'metadata_spreadsheet')
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
