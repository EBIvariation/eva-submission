import ftplib
import os
import shutil
import subprocess

import requests
import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission.biosamples_submission import SampleMetadataSubmitter
from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import read_md5
from eva_submission.xlsx_to_xml.xlsx_to_ENA_xml import process_metadata_spreadsheet


class HackFTP_TLS(ftplib.FTP_TLS):
    """
    Hack from https://stackoverflow.com/questions/14659154/ftpes-session-reuse-required
    to work around bug in Python standard library: https://bugs.python.org/issue19500
    Explicit FTPS, with shared TLS session
    """
    def ntransfercmd(self, cmd, rest=None):
        conn, size = ftplib.FTP.ntransfercmd(self, cmd, rest)
        if self._prot_p:
            conn = self.context.wrap_socket(conn,
                                            server_hostname=self.host,
                                            session=self.sock.session)  # this is the fix
        return conn, size


class EloadBrokering(Eload):

    def __init__(self, eload_number: int, vcf_files: list = None, metadata_file: str = None):
        super().__init__(eload_number)
        if 'validation' not in self.eload_cfg:
            self.eload_cfg['validation'] = {}
        if vcf_files or metadata_file:
            self.eload_cfg.set('validation', 'valid', value={'Force': True, 'date': self.now})
            if vcf_files:
                self.eload_cfg.set('validation', 'valid', 'vcf_files', value=[os.path.abspath(v) for v in vcf_files])
            if metadata_file:
                self.eload_cfg.set('validation', 'valid', 'metadata_spreadsheet', value=os.path.abspath(metadata_file))

    def broker(self):
        # Reset previous values that could have been set before
        self.eload_cfg['brokering'] = {}
        output_dir = self._run_brokering_prep_workflow()
        self._collect_brokering_worklflow_results(output_dir)
        shutil.rmtree(output_dir)

        # TODO: Test if biosample upload is required
        # Find sampletab file
        self.upload_to_bioSamples(self.eload_cfg['validation']['valid']['metadata_spreadsheet'])
        ena_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
        self.update_metadata_from_config(self.eload_cfg['validation']['valid']['metadata_spreadsheet'], ena_spreadsheet)

        submission_file, project_file, analysis_file = process_metadata_spreadsheet(ena_spreadsheet, self._get_dir('ena'), self.eload)

        # Upload the VCF to ENA FTP
        self.upload_vcf_files_to_ena_ftp()

        # Upload XML to ENA

    def upload_vcf_files_to_ena_ftp(self):
        print('Connect to %s' % cfg.query('ena', 'ftphost'))
        ftps = HackFTP_TLS()
        host, port = cfg.query('ena', 'ftphost').split(':')
        ftps.connect(host, port=int(cfg.query('ena', 'ftpport', ret_default=21)))
        ftps.login(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
        ftps.prot_p()
        if self.eload not in ftps.nlst():
            self.info('Create %s directory' % self.eload)
            ftps.mkd(self.eload)
        ftps.cwd(self.eload)
        for vcf_file in self.eload_cfg['brokering']['vcf_files']:
            vcf_file_name = os.path.basename(vcf_file)
            self.info('Upload %s to FTP' % vcf_file_name)
            with open(vcf_file, 'rb') as open_file:
                ftps.storbinary('STOR %s' % vcf_file_name, open_file)
            vcf_file_index = self.eload_cfg['brokering']['vcf_files'][vcf_file]['index']
            vcf_file_index_name = os.path.basename(vcf_file_index)
            self.info('Upload %s to FTP' % vcf_file_index_name)
            with open(vcf_file_index, 'rb') as open_file:
                ftps.storbinary('STOR %s' % vcf_file_index_name, open_file)

    def upload_xml_files_to_ena(self, submission_file, project_file, analysis_file):
        response = requests.post(
            cfg.query('ena', 'submit_url'),
            files=dict(SUBMISSION=submission_file, PROJECT=project_file, ANALYSIS=analysis_file)
        )
        # TODO: complete the parsing of the response

    def upload_to_bioSamples(self, metadata_spreadsheet):
        sample_tab_submitter = SampleMetadataSubmitter(metadata_spreadsheet)
        sample_name_to_accession = sample_tab_submitter.submit_to_bioSamples()
        self.eload_cfg['brokering']['Biosamples'] = sample_name_to_accession

    def _run_brokering_prep_workflow(self):
        output_dir = self.create_temp_output_directory()
        brokering_config = {
            'vcf_files': self.eload_cfg['validation']['valid']['vcf_files'],
            'output_dir': output_dir,
            'executable': cfg['executable']
        }
        # run the validation
        brokering_confg_file = os.path.join(self.eload_dir, 'brokering_confg_file.yaml')
        with open(brokering_confg_file, 'w') as open_file:
            yaml.safe_dump(brokering_config, open_file)
        validation_script = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'nextflow', 'prepare_brokering.nf')
        try:
            command_utils.run_command_with_output(
                'Start Nextflow brokering preparation process',
                ' '.join((
                    cfg['executable']['nextflow'], validation_script,
                    '-params-file', brokering_confg_file,
                    '-work-dir', output_dir
                ))
            )
        except subprocess.CalledProcessError as e:
            self.error('Nextflow pipeline failed: aborting brokering')
            raise e
        return output_dir

    def _collect_brokering_worklflow_results(self, output_dir):
        # Collect information from the output and summarise in the config
        nextflow_vcf_output = os.path.join(output_dir, 'output')
        for vcf_file in self.eload_cfg.query('validation', 'valid', 'vcf_files'):
            vcf_file_name = os.path.basename(vcf_file)
            if not vcf_file_name.endswith('.gz'):
                vcf_file_name = vcf_file_name + '.gz'

            ovcf_file = os.path.join(self._get_dir('ena'), vcf_file_name)
            os.rename(os.path.join(nextflow_vcf_output, vcf_file_name), ovcf_file)
            os.rename(os.path.join(output_dir, vcf_file_name) + '.md5', ovcf_file + '.md5')

            ifile = os.path.join(output_dir, vcf_file_name + '.csi')
            o_index_file = os.path.join(self._get_dir('ena'), vcf_file_name + '.csi')
            os.rename(ifile, o_index_file)
            os.rename(ifile + '.md5', o_index_file + '.md5')

            self.eload_cfg.set('brokering', 'vcf_files', ovcf_file, value={
                'original_vcf': vcf_file,
                'md5': read_md5(ovcf_file+'.md5'),
                'index': o_index_file,
                'index_md5': read_md5(o_index_file+'.md5'),
            })

    # def update_metadata_from_config(self, spreadsheet):
    #     reader = EVAXLSReader(spreadsheet)
    #     single_analysis_alias = None
    #     if len(reader.analysis) == 1 :
    #         single_analysis_alias = reader.analysis[0].get('Analysis Alias')
    #
    #     sample_rows = []
    #     for sample_row in reader.samples:
    #         sample_rows.append({
    #             'row_num': sample_row.get('row_num'),
    #             'Analysis Alias': sample_row.get('Analysis Alias') or single_analysis_alias,
    #             'Sample ID': sample_row.get('Sample Name'),
    #             'Sample Accession': self.eload_cfg['brokering']['Biosamples'][sample_row.get('Sample Name')]
    #         })
    #
    #     file_rows = []
    #     file_to_row = {}
    #     for file_row in reader.files:
    #         file_to_row[file_row['File Name']] = file_row
    #
    #     for vcf_file in self.eload_cfg['brokering']['vcf_files']:
    #         original_vcf_file = self.eload_cfg['brokering']['vcf_files'][vcf_file]['original_vcf']
    #         file_row = file_to_row.get(os.path.basename(original_vcf_file), default={})
    #         # Add the vcf file
    #         file_rows.append({
    #             'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
    #             'File Name': self.eload + '/' + vcf_file,
    #             'File Type': 'vcf',
    #             'MD5': self.eload_cfg['brokering']['vcf_files'][vcf_file]['md5']
    #         })
    #
    #         # Add the index file
    #         file_rows.append({
    #             'Analysis Alias': file_row.get('Analysis Alias') or single_analysis_alias,
    #             'File Name': self.eload + '/' + os.path.basename(self.eload_cfg['brokering']['vcf_files'][vcf_file]['index']),
    #             'File Type': 'tabix',
    #             'MD5': self.eload_cfg['brokering']['vcf_files'][vcf_file]['index_md5']
    #         })
    #
    #     eva_xls_writer = EVAXLSWriter(spreadsheet)
    #     eva_xls_writer.set_samples(sample_rows)
    #     eva_xls_writer.set_files(file_rows)
    #     output_spreadsheet = os.path.join(self._get_dir('ena'), 'metadata_spreadsheet.xlsx')
    #     eva_xls_writer.save(output_spreadsheet)
    #     return output_spreadsheet

