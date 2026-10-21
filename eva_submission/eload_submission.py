import json
import os
import random
import string
from datetime import datetime

from cached_property import cached_property
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from ebi_eva_internal_pyutils.metadata_utils import get_metadata_connection_handle
from packaging import version

from eva_sub_cli_processing.sub_cli_utils import put_to_sub_ws, sub_ws_url_build
from eva_submission import __version__
from eva_submission.biosample_submission.sample_record import SampleRecord, records_from_dicts
from eva_submission.config_migration import upgrade_version_0_1, upgrade_version_1_14_to_1_15, \
    upgrade_version_1_15_to_1_16
from eva_submission.eload_utils import get_hold_date_from_ena
from eva_submission.submission_config import EloadConfig
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter

directory_structure = {
    'vcf': '10_submitted/vcf_files',
    'metadata': '10_submitted/metadata_file',
    'validation': '13_validation',
    'normalisation_check': '13_validation/normalisation',
    'sv_check': '13_validation/structural_variant',
    'naming_convention_check': '13_validation/naming_convention',
    'eva_sub_cli': '13_validation/eva_sub_cli',
    'biosamples': '18_brokering/biosamples',
    'ena': '18_brokering/ena',
    'scratch': '20_scratch'
}
eload_logging_files = set()


class Eload(AppLogger):
    def __init__(self, eload_number: int, config_object: EloadConfig = None):
        self.eload_num = eload_number
        self.eload = f'ELOAD_{eload_number}'
        self.eload_dir = os.path.abspath(os.path.join(cfg['eloads_dir'], self.eload))
        self.nobackup_eload_dir = os.path.abspath(os.path.join(cfg['nobackup_eloads_dir'], self.eload))
        self.config_path = os.path.join(self.eload_dir, '.' + self.eload + '_config.yml')
        if config_object:
            self.eload_cfg = config_object
        else:
            self.eload_cfg = EloadConfig(self.config_path)

        os.makedirs(self.eload_dir, exist_ok=True)
        os.makedirs(self.nobackup_eload_dir, exist_ok=True)
        for k in directory_structure:
            os.makedirs(self._get_dir(k), exist_ok=True)
        self.create_log_file()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.eload_cfg.write()

    @property
    def metadata_connection_handle(self):
        return get_metadata_connection_handle(cfg['maven']['environment'], cfg['maven']['settings_file'])

    def create_nextflow_temp_output_directory(self, base=None):
        random_string = ''.join(random.choice(string.ascii_letters) for i in range(6))
        if base is None:
            base = self.nobackup_eload_dir
        output_dir = os.path.join(base, 'nextflow_output_' + random_string)
        os.makedirs(output_dir)
        return output_dir

    def _get_dir(self, key):
        return os.path.join(self.eload_dir, directory_structure[key])

    @cached_property
    def now(self):
        return datetime.now()

    @cached_property
    def submission_id(self):
        return self.eload_cfg.query('submission', 'submission_id')

    def _unique_alias(self, alias):
        if alias and not alias.startswith(self.eload):
            return f'{self.eload}_{alias}'
        return alias

    def _undo_unique_alias(self, alias):
        if alias.startswith(self.eload + '_'):
            return alias[len(self.eload) + 1:]
        return alias

    def _stored_sample_records(self):
        """The sample records stored in the config, or None if the samples were not brokered with records."""
        record_dicts = self.eload_cfg.query('brokering', 'Biosamples', 'sample_records')
        if record_dicts is None:
            return None
        return records_from_dicts(record_dicts)

    def sample_records(self):
        """
        Returns the records of the samples brokered to BioSamples.
        ELOADs brokered before the records were introduced only have a sample name to accession dict ('Samples'):
        it is converted to records that have no analysis information.
        """
        sample_records = self._stored_sample_records()
        if sample_records is not None:
            return sample_records
        sample_name_to_accession = self.eload_cfg.query('brokering', 'Biosamples', 'Samples', ret_default={})
        return [
            SampleRecord.from_legacy(sample_name, accession)
            for sample_name, accession in sample_name_to_accession.items()
        ]

    def sample_records_for_analysis(self, analysis_alias):
        """Records belonging to the analysis. Records without analysis information belong to all analyses."""
        return [
            record for record in self.sample_records()
            if record.in_analysis(analysis_alias, normalise_alias=self._unique_alias)
        ]

    def sample_name_to_accession(self):
        """Map of sample name in VCF to accession across all analyses. Only used by the legacy spreadsheet path."""
        sample_name_to_accession = {}
        for record in self.sample_records():
            if record.sample_in_vcf and record.sample_in_vcf not in sample_name_to_accession:
                sample_name_to_accession[record.sample_in_vcf] = record.accession
        return sample_name_to_accession

    def sample_name_to_accession_for_analysis(self, analysis_alias):
        """Map of sample name in VCF to accession for one analysis. Used to load genotype analyses."""
        sample_name_to_accession = {}
        for record in self.sample_records_for_analysis(analysis_alias):
            sample_name = record.sample_in_vcf
            if not sample_name:
                continue
            if sample_name in sample_name_to_accession and sample_name_to_accession[sample_name] != record.accession:
                raise ValueError(f'Sample {sample_name} in analysis {analysis_alias} is associated with multiple '
                                 f'accessions: {sample_name_to_accession[sample_name]}, {record.accession}')
            sample_name_to_accession[sample_name] = record.accession
        return sample_name_to_accession

    def sample_accession_to_name_for_analysis(self, analysis_alias):
        """
        Map of accession to sample name for one analysis. Used to load allele frequency analyses.
        The name is the record's label: sample name in VCF if provided, else the BioSample name, else the accession.
        """
        sample_accession_to_name = {}
        for record in self.sample_records_for_analysis(analysis_alias):
            if record.accession in sample_accession_to_name:
                if sample_accession_to_name[record.accession] != record.label:
                    self.warning(f'Sample {record.accession} in analysis {analysis_alias} has multiple names: '
                                 f'{sample_accession_to_name[record.accession]} and {record.label}. '
                                 f'Keep the first one.')
                continue
            sample_accession_to_name[record.accession] = record.label
        return sample_accession_to_name

    def create_log_file(self):
        logfile_name = os.path.join(self.eload_dir, str(self.eload) + "_submission.log")
        if logfile_name not in eload_logging_files:
            log_cfg.add_file_handler(logfile_name)
            eload_logging_files.add(logfile_name)

    def upgrade_to_new_version_if_needed(self, analysis_alias=None):
        """
        Upgrades configs or directory structure to the current version, it supports:
         - making a backup
         - using the provided analysis alias for all vcf files (pre version 1)
         - reformat nextflow directories in the config (pre version 1.15)
         - link project subdirectories  to the eload directories and update the config (pre version 1.16)
        """
        if 'version' not in self.eload_cfg:
            self.debug(f'No version found in config, upgrading to version {__version__}.')
            self.eload_cfg.backup()
            upgrade_version_0_1(self.eload_cfg, analysis_alias)
            upgrade_version_1_14_to_1_15(self.eload_cfg)
            upgrade_version_1_15_to_1_16(self.eload_cfg, self.eload_dir)
            self.eload_cfg.write()
        elif version.parse(self.eload_cfg.query('version')) < version.parse("1.15"):
            self.debug(f'Pre version 1.15, upgrading to version from {version} to {__version__}.')
            self.eload_cfg.backup()
            upgrade_version_1_14_to_1_15(self.eload_cfg)
            upgrade_version_1_15_to_1_16(self.eload_cfg, self.eload_dir)
            self.eload_cfg.write()
        elif version.parse(self.eload_cfg.query('version')) < version.parse("1.16"):
            upgrade_version_1_15_to_1_16(self.eload_cfg, self.eload_dir)
        else:
            self.debug(f"Config is version {self.eload_cfg.query('version')}, not upgrading.")

    def update_config_with_hold_date(self, project_accession, project_alias=None):
        hold_date = get_hold_date_from_ena(project_accession, project_alias)
        self.eload_cfg.set('brokering', 'ena', 'hold_date', value=hold_date)

    def _accessioned_sample_objects(self, samples, sample_records):
        """The sample records are in the same order as the samples in the metadata: sample i gets record i."""
        if len(samples) != len(sample_records):
            raise ValueError(f'The metadata contains {len(samples)} samples but {len(sample_records)} were brokered '
                             f'to BioSamples')
        sample_objects = []
        for sample_obj, record in zip(samples, sample_records):
            accession_in_metadata = sample_obj.get('bioSampleAccession')
            if accession_in_metadata and not record.is_derived and accession_in_metadata != record.accession:
                raise ValueError(f'Sample {accession_in_metadata} in the metadata does not match the brokered sample '
                                 f'{record.accession}: the metadata was modified after brokering')
            analysis_aliases = sample_obj.get('analysisAlias') or []
            if isinstance(analysis_aliases, str):
                analysis_aliases = [analysis_aliases]
            sample_object = {
                'analysisAlias': [self._unique_alias(a) for a in analysis_aliases],
                'bioSampleAccession': record.accession
            }
            if record.sample_in_vcf:
                sample_object['sampleInVCF'] = record.sample_in_vcf
            sample_objects.append(sample_object)
        return sample_objects

    def _legacy_accessioned_sample_objects(self, samples):
        """ELOADs brokered before the sample records were introduced use the sample name to accession dict."""
        sample_objects = []
        for sample_obj in samples:
            if self.eload_cfg.query('brokering', 'Biosamples', 'Samples', sample_obj.get('sampleInVCF')):
                sample_objects.append({
                    'analysisAlias': [self._unique_alias(a) for a in sample_obj.get('analysisAlias')],
                    'sampleInVCF': sample_obj.get('sampleInVCF'),
                    'bioSampleAccession': self.eload_cfg['brokering']['Biosamples']['Samples'][sample_obj.get('sampleInVCF')]
                })
            else:
                sample_obj['analysisAlias'] = [self._unique_alias(a) for a in sample_obj.get('analysisAlias')]
                sample_objects.append(sample_obj)
        return sample_objects

    def update_metadata_json(self, input_json_file, output_json_file, existing_project=None):
        with open(input_json_file) as open_file:
            metadata_json = json.load(open_file)
        sample_records = self._stored_sample_records()
        if sample_records is not None:
            sample_objects = self._accessioned_sample_objects(metadata_json.get('sample'), sample_records)
        else:
            sample_objects = self._legacy_accessioned_sample_objects(metadata_json.get('sample'))

        file_objects = []
        analyses = self.eload_cfg['brokering']['analyses']
        for analysis in analyses:
            for vcf_file_name in analyses[analysis]['vcf_files']:
                vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                # Add the vcf file
                file_objects.append({
                    'analysisAlias': self._unique_alias(analysis),
                    'fileName': self.eload + '/' + os.path.basename(vcf_file_name),
                    'fileType': 'vcf',
                    'md5': vcf_file_info['md5']
                })

                # Add the index file
                file_objects.append({
                    'analysisAlias': self._unique_alias(analysis),
                    'fileName': self.eload + '/' + os.path.basename(vcf_file_info['csi']),
                    'fileType': 'csi',
                    'md5': vcf_file_info['csi_md5']
                })

        analysis_objects = []
        for analysis_obj in metadata_json.get('analysis'):
            analysis_obj['analysisAlias'] = self._unique_alias(analysis_obj['analysisAlias'])
            validation_assembly = self.eload_cfg.query(
                'brokering', 'analyses', analysis_obj['analysisAlias'], 'assembly_accession'
            )
            if validation_assembly and validation_assembly != analysis_obj['referenceGenome']:
                analysis_obj['referenceGenome'] = validation_assembly
            analysis_objects.append(analysis_obj)

        metadata_json['sample'] = sample_objects
        metadata_json['files'] = file_objects
        metadata_json['analysis'] = analysis_objects

        if existing_project:
            self.warning('Information about the project will be overwritten ' + str(metadata_json.get('project')))
            metadata_json['project'] = {'projectAccession' : existing_project}

        with open(output_json_file, 'w') as open_file:
            json.dump(metadata_json, open_file, indent=4)

    def update_metadata_spreadsheet(self, input_spreadsheet, output_spreadsheet=None, existing_project=None):
        reader = EvaXlsxReader(input_spreadsheet)
        single_analysis_alias = None
        if len(reader.analysis) == 1:
            single_analysis_alias = self._unique_alias(reader.analysis[0].get('Analysis Alias'))

        sample_name_to_accession = self.sample_name_to_accession()
        sample_rows = []
        for sample_row in reader.samples:
            if sample_name_to_accession.get(sample_row.get('Sample Name')):
                sample_rows.append({
                    'row_num': sample_row.get('row_num'),
                    'Analysis Alias': self._unique_alias(sample_row.get('Analysis Alias')) or single_analysis_alias,
                    'Sample ID': sample_row.get('Sample Name'),
                    'Sample Accession': sample_name_to_accession[sample_row.get('Sample Name')]
                })
            else:
                sample_row['Analysis Alias'] = self._unique_alias(sample_row['Analysis Alias'])
                sample_rows.append(sample_row)

        file_rows = []
        analyses = self.eload_cfg['brokering']['analyses']
        for analysis in analyses:
            for vcf_file_name in analyses[analysis]['vcf_files']:
                vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                # Add the vcf file
                file_rows.append({
                    'Analysis Alias': self._unique_alias(analysis),
                    'File Name': self.eload + '/' + os.path.basename(vcf_file_name),
                    'File Type': 'vcf',
                    'MD5': vcf_file_info['md5']
                })

                # Add the index file
                file_rows.append({
                    'Analysis Alias': self._unique_alias(analysis),
                    'File Name': self.eload + '/' + os.path.basename(vcf_file_info['csi']),
                    'File Type': 'csi',
                    'MD5': vcf_file_info['csi_md5']
                })

        project_row = reader.project
        if existing_project:
            project_row['Project Alias'] = existing_project
        elif self.eload not in project_row['Project Alias']:
            # Add the eload id to ensure that the project alias is unique
            project_row['Project Alias'] = self._unique_alias(project_row['Project Alias'])

        analysis_rows = reader.analysis
        for analysis_row in analysis_rows:
            # Add the eload id to ensure that the analysis alias is unique
            analysis_row['Analysis Alias'] = self._unique_alias(analysis_row['Analysis Alias'])
            # Ensure that the reference used in the brokering is the same as the one used during validation
            analysis_row['Reference'] = self.eload_cfg.query(
                'brokering', 'analyses', analysis_row['Analysis Alias'], 'assembly_accession'
            )

        if output_spreadsheet:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet, output_spreadsheet)
        else:
            eva_xls_writer = EvaXlsxWriter(input_spreadsheet)
        eva_xls_writer.set_project(project_row)
        eva_xls_writer.set_analysis(analysis_rows)
        eva_xls_writer.set_samples(sample_rows)
        eva_xls_writer.set_files(file_rows)
        eva_xls_writer.save()
        return output_spreadsheet

    @staticmethod
    def _check_pass_or_fail(check_dict):
        if check_dict and check_dict.get('forced'):
            return 'FORCED'
        if check_dict and check_dict.get('pass'):
            return 'PASS'
        return 'FAIL'

    def update_submission_status(self, submission_step, step_status):
        try:
            if self.submission_id:
                put_to_sub_ws(sub_ws_url_build('admin', 'submission-process', self.submission_id,
                                               submission_step, step_status))
            else:
                self.warning(f'Could not update submission status ({submission_step} - {step_status}) as no submission id provided for Eload {self.eload}')
        except Exception as e:
            self.warning(f"Could not update status {step_status} for the submission step '{submission_step}'. Error {e}")

    def check_eload_qc_is_successful(self):
        from eva_submission.submission_qc_checks import EloadQC
        qc_results = self.eload_cfg.query(EloadQC.config_section)
        if not qc_results:
            return False
        for check in qc_results:
            if qc_results[check] not in EloadQC.SUCCESSFUL_RESULTS:
                return False
        return True
