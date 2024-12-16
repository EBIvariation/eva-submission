import csv
import os
import shutil

from eva_submission.ENA_submission.upload_to_ENA import ENAUploader, ENAUploaderAsync
from eva_submission.biosample_submission.biosamples_submitters import SampleReferenceSubmitter, \
    SampleSubmitter
from eva_sub_cli_processing.sub_cli_submission import SubCli


class SubCliBrokering(SubCli):

    def broker(self, brokering_tasks_to_force=None, existing_project=None, async_upload=False, dry_ena_upload=False):
        """Run the brokering process"""
        self.upload_to_bioSamples()
        self.broker_to_ena(force=('ena' in brokering_tasks_to_force), existing_project=existing_project,
                           async_upload=async_upload, dry_ena_upload=dry_ena_upload)
        self.update_biosamples_with_study(force=('update_biosamples' in brokering_tasks_to_force))

    def prepare_brokering(self, force=False):
        valid_analyses = self.eload_cfg.query('validation', 'valid', 'analyses', ret_default=[])
        if not all([
            self.eload_cfg.query('brokering', 'analyses', analysis, 'vcf_files')
            for analysis in valid_analyses
        ]) or force:
            output_dir = self._run_brokering_prep_workflow()
            self._collect_brokering_prep_results(output_dir)
            shutil.rmtree(output_dir)
        else:
            self.info('Preparation has already been run, Skip!')

    def broker_to_ena(self, force=False, existing_project=None, async_upload=False, dry_ena_upload=False):
        if not self.eload_cfg.query('brokering', 'ena', 'pass') or force:
            ena_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
            # Set the project in the metadata sheet which is then converted to XML
            self.update_metadata_spreadsheet(self.eload_cfg['validation']['valid']['metadata_spreadsheet'],
                                             ena_spreadsheet, existing_project)
            if async_upload:
                ena_uploader = ENAUploaderAsync(self.eload, ena_spreadsheet, self._get_dir('ena'))
            else:
                ena_uploader = ENAUploader(self.eload, ena_spreadsheet, self._get_dir('ena'))

            if ena_uploader.converter.is_existing_project:
                # Set the project in the config, based on the spreadsheet
                self.eload_cfg.set('brokering', 'ena', 'PROJECT', value=ena_uploader.converter.existing_project)
                self.eload_cfg.set('brokering', 'ena', 'existing_project', value=True)

            # Upload the VCF to ENA FTP
            files_to_upload = []
            analyses = self.eload_cfg['brokering']['analyses']
            for analysis in analyses:
                for vcf_file_name in analyses[analysis]['vcf_files']:
                    vcf_file_info = self.eload_cfg['brokering']['analyses'][analysis]['vcf_files'][vcf_file_name]
                    files_to_upload.append(vcf_file_info['output_vcf_file'])
                    files_to_upload.append(vcf_file_info['csi'])
            if dry_ena_upload:
                self.info(f'Would have uploaded the following files to FTP: \n' + "\n".join(files_to_upload))
            else:
                ena_uploader.upload_vcf_files_to_ena_ftp(files_to_upload)
            # Upload XML to ENA
            ena_uploader.upload_xml_files_to_ena(dry_ena_upload)
            if not dry_ena_upload:
                # Update the project accession in case we're working with existing project
                # We should not be uploading additional analysis in th same ELOAD so no need to update
                pre_existing_project = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
                if pre_existing_project and 'PROJECT' not in ena_uploader.results:
                    ena_uploader.results['PROJECT'] = pre_existing_project
                self.eload_cfg.set('brokering', 'ena', value=ena_uploader.results)
                self.eload_cfg.set('brokering', 'ena', 'date', value=self.now)
                self.eload_cfg.set('brokering', 'ena', 'hold_date', value=ena_uploader.converter.hold_date)
                self.eload_cfg.set('brokering', 'ena', 'pass', value=not bool(ena_uploader.results['errors']))
        else:
            self.info('Brokering to ENA has already been run, Skip!')

    def upload_to_bioSamples(self, force=False):
        sample_submitter = SampleSubmitter(('create',))
        sample_submitter.sample_data = self.submission.submission_detail.get('samples')
        sample_name_to_accession = sample_submitter.submit_to_bioSamples()
        # Check whether all samples have been accessioned
        passed = (
            bool(sample_name_to_accession)
            and all(sample_name in sample_name_to_accession for sample_name in sample_submitter.all_sample_names())
        )
        if not passed:
            raise ValueError(f'Not all samples were successfully brokered to BioSamples! '
                             f'Found {len(sample_name_to_accession)} and expected '
                             f'{len(sample_metadata_submitter.all_sample_names())}. '
                             f'Missing samples are '
                             f'{[sample_name for sample_name in sample_metadata_submitter.all_sample_names() if sample_name not in sample_name_to_accession]}')

    def update_biosamples_with_study(self, force=False):
        if not self.eload_cfg.query('brokering', 'Biosamples', 'backlinks') or force:
            biosample_accession_list = self.eload_cfg.query('brokering', 'Biosamples', 'Samples').values()
            project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
            if project_accession:
                self.info(f'Add external reference to {len(biosample_accession_list)} BioSamples.')
                sample_reference_submitter = SampleReferenceSubmitter(biosample_accession_list, project_accession)
                sample_reference_submitter.submit_to_bioSamples()
                self.eload_cfg.set('brokering', 'Biosamples', 'backlinks', value=project_accession)
        else:
            self.info('Adding external reference to BioSamples has already been done, Skip!')
