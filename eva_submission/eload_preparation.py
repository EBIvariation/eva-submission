import glob
import os
import shutil
import requests
import json

from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.config_utils import get_contig_alias_db_creds_for_profile


from eva_submission.eload_submission import Eload, directory_structure
from eva_submission.eload_utils import resolve_accession_from_text, get_reference_fasta_and_report, NCBIAssembly
from eva_submission.submission_in_ftp import FtpDepositBox
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter


class EloadPreparation(Eload):

    def copy_from_ftp(self, ftp_box, submitter):
        box = FtpDepositBox(ftp_box, submitter)

        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        for vcf_file in box.vcf_files:
            dest = os.path.join(vcf_dir, os.path.basename(vcf_file))
            shutil.copyfile(vcf_file, dest)

        if box.most_recent_metadata:
            if len(box.metadata_files) != 1:
                self.warning('Found %s metadata file in the FTP. Will use the most recent one', len(box.metadata_files))
            metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
            dest = os.path.join(metadata_dir, os.path.basename(box.most_recent_metadata))
            shutil.copyfile(box.most_recent_metadata, dest)
        else:
            self.error('No metadata file in the FTP: %s', box.deposit_box)

        for other_file in box.other_files:
            self.warning('File %s will not be treated', other_file)

    def replace_values_in_metadata(self, taxid=None, reference_accession=None):
        """Find and Replace the value in the metadata spreadsheet with the one provided """
        input_spreadsheet = self.eload_cfg.query('submission', 'metadata_spreadsheet')
        reader = EvaXlsxReader(input_spreadsheet)

        # This will write the spreadsheet in place of the existing one
        eva_xls_writer = EvaXlsxWriter(input_spreadsheet)
        if taxid:
            project = reader.project
            project['Tax ID'] = taxid
            eva_xls_writer.set_project(project)
        if reference_accession:
            analysis_rows = []
            for analysis in reader.analysis:
                analysis['Reference'] = reference_accession
                analysis_rows.append(analysis)
            eva_xls_writer.set_analysis(analysis_rows)
        eva_xls_writer.save()

    def detect_all(self, taxid=None, reference_accession=None):
        # New detection so the config should be backup and reset
        if not self.eload_cfg.is_empty():
            self.debug('Config will be reset')
            self.eload_cfg.backup()
            self.eload_cfg.clear()
        self.detect_submitted_metadata()
        if taxid or reference_accession:
            self.replace_values_in_metadata(taxid=taxid, reference_accession=reference_accession)
        self.check_submitted_filenames()
        self.detect_metadata_attributes()
        self.find_genome()

    def detect_submitted_metadata(self):
        metadata_dir = os.path.join(self.eload_dir, directory_structure['metadata'])
        metadata_spreadsheets = glob.glob(os.path.join(metadata_dir, '*.xlsx'))
        if len(metadata_spreadsheets) != 1:
            self.critical('Found %s spreadsheet in %s', len(metadata_spreadsheets), metadata_dir)
            raise ValueError('Found %s spreadsheet in %s'% (len(metadata_spreadsheets), metadata_dir))
        self.eload_cfg.set('submission', 'metadata_spreadsheet', value=metadata_spreadsheets[0])

    def check_submitted_filenames(self):
        """Compares submitted vcf filenames with those in metadata sheet, and amends the metadata when possible."""
        vcf_dir = os.path.join(self.eload_dir, directory_structure['vcf'])
        uncompressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf'))
        compressed_vcf = glob.glob(os.path.join(vcf_dir, '*.vcf.gz'))
        submitted_vcfs = [os.path.basename(vcf) for vcf in uncompressed_vcf + compressed_vcf]
        if len(submitted_vcfs) < 1:
            raise FileNotFoundError('Could not locate vcf file in %s', vcf_dir)

        eva_files_sheet = self.eload_cfg.query('submission', 'metadata_spreadsheet')
        eva_xls_reader = EvaXlsxReader(eva_files_sheet)
        spreadsheet_vcfs = [
            os.path.basename(row['File Name']) for row in eva_xls_reader.files
            if row['File Type'] == 'vcf' or row['File Name'].endswith('.vcf') or row['File Name'].endswith('.vcf.gz')
        ]

        if sorted(spreadsheet_vcfs) != sorted(submitted_vcfs):
            self.warning('VCF files found in the spreadsheet does not match the ones submitted. '
                         'Submitted VCF will be added to the spreadsheet')
            self.debug(f'Difference between spreadsheet vcfs and submitted vcfs: '
                       f'{", ".join(set(spreadsheet_vcfs).difference(set(submitted_vcfs)))}')
            self.debug(f'Difference between submitted vcfs and spreadsheet vcfs: '
                       f'{", ".join(set(submitted_vcfs).difference(set(spreadsheet_vcfs)))}')
            analysis_alias = ''
            if len(eva_xls_reader.analysis) == 1:
                analysis_alias = eva_xls_reader.analysis[0].get('Analysis Alias') or ''
            elif len(eva_xls_reader.analysis) > 1:
                self.error("Multiple analyses found, can't add submitted VCF to spreadsheet")
                raise ValueError("Multiple analyses found, can't add submitted VCF to spreadsheet")
            eva_xls_writer = EvaXlsxWriter(eva_files_sheet)
            eva_xls_writer.set_files([
                {
                    'File Name': vcf_file,
                    'File Type': 'vcf',
                    'Analysis Alias': analysis_alias,
                    'MD5': ''  # Dummy md5 for now
                } for vcf_file in submitted_vcfs
            ])
            eva_xls_writer.save()

    def detect_metadata_attributes(self):
        eva_metadata = EvaXlsxReader(self.eload_cfg.query('submission', 'metadata_spreadsheet'))
        analysis_reference = {}
        for analysis in eva_metadata.analysis:
            reference_txt = analysis.get('Reference')
            assembly_accessions = resolve_accession_from_text(reference_txt) if reference_txt else None
            if not assembly_accessions:
                assembly_accession = None
            elif len(assembly_accessions) == 1:
                assembly_accession = assembly_accessions[0]
            else:
                self.warning(f"Multiple assemblies found for {analysis.get('Analysis Alias')}: {', '.join(assembly_accessions)} ")
                assembly_accession = sorted(assembly_accessions)[-1]
                self.warning(f"Will use the most recent assembly: {assembly_accession}")

            if assembly_accession:
                analysis_reference[analysis.get('Analysis Alias')] = {'assembly_accession': assembly_accession,
                                                                      'vcf_files': []}
            else:
                self.error(f"Reference is missing for Analysis {analysis.get('Analysis Alias')}")

        for file in eva_metadata.files:
            if file.get("File Type") == 'vcf':
                file_full = os.path.join(self.eload_dir, directory_structure['vcf'], file.get("File Name"))
                analysis_alias = file.get("Analysis Alias")
                analysis_reference[analysis_alias]['vcf_files'].append(file_full)
        self.eload_cfg.set('submission', 'analyses', value=analysis_reference)

        taxonomy_id = eva_metadata.project.get('Tax ID')
        if taxonomy_id and (isinstance(taxonomy_id, int) or taxonomy_id.isdigit()):
            self.eload_cfg.set('submission', 'taxonomy_id', value=int(taxonomy_id))
            scientific_name = get_scientific_name_from_ensembl(taxonomy_id)
            self.eload_cfg.set('submission', 'scientific_name', value=scientific_name)
        else:
            if taxonomy_id:
                self.error('Taxonomy id %s is invalid:', taxonomy_id)
            else:
                self.error('Taxonomy id is missing for the submission')

    def find_genome(self):
        scientific_name = self.eload_cfg.query('submission', 'scientific_name')
        analyses = self.eload_cfg.query('submission', 'analyses')

        contig_alias_url, contig_alias_user, contig_alias_pass = get_contig_alias_db_creds_for_profile(
            cfg['maven']['environment'], cfg['maven']['settings_file'])

        contig_alias_payload = []

        if scientific_name:
            for analysis_alias in analyses:
                assembly_accession = self.eload_cfg.query('submission', 'analyses', analysis_alias, 'assembly_accession')

                if NCBIAssembly.is_assembly_accession_format(assembly_accession):
                    contig_alias_payload.append(assembly_accession)

                assembly_fasta_path, assembly_report_path = get_reference_fasta_and_report(scientific_name,
                                                                                           assembly_accession)
                if assembly_report_path:
                    self.eload_cfg.set('submission', 'analyses', analysis_alias, 'assembly_report', value=assembly_report_path)
                else:
                    self.warning(f'Assembly report was not set for {assembly_accession}')
                self.eload_cfg.set('submission', 'analyses', analysis_alias, 'assembly_fasta', value=assembly_fasta_path)

            for assembly in contig_alias_payload:
                response_get = requests.get(os.path.join(contig_alias_url, assembly),
                                            auth=(contig_alias_user, contig_alias_pass))

                response_get_data = response_get.json()

                if response_get_data['page']['size'] != 0:
                    response_put = requests.put(os.path.join(contig_alias_url, assembly), auth=(contig_alias_user, contig_alias_pass), json=assembly)
                    response_put.raise_for_status()

        else:
            self.error('No scientific name specified')
