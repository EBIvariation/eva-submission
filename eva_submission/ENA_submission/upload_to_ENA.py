import ftplib
import os
import time
from xml.etree import ElementTree as ET

import requests
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.config import cfg
from requests.auth import HTTPBasicAuth
from retry import retry

from eva_submission.ENA_submission.xlsx_to_ENA_xml import EnaXlsxConverter
from eva_submission.eload_utils import get_file_content


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


class ENAUploader(AppLogger):
    def __init__(self, eload, ena_spreadsheet, output_dir):
        self.eload = eload
        self.results = {}
        self.converter = EnaXlsxConverter(ena_spreadsheet, output_dir, self.eload)
        self.ena_auth = HTTPBasicAuth(cfg.query('ena', 'username'), cfg.query('ena', 'password'))

    @retry(exceptions=ftplib.all_errors, tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def upload_vcf_files_to_ena_ftp(self, files_to_upload):
        host = cfg.query('ena', 'ftphost')
        # Heuristic to set the expected timeout assuming 10Mb/s upload speed but no less than 30 sec
        # and no more than an hour
        max_file_size = max([os.path.getsize(f) for f in files_to_upload])
        timeout = min(max(int(max_file_size / 10000000), 30), 3600)
        self.info(f'Connect to {host} with timeout: {timeout}')
        with HackFTP_TLS() as ftps:
            # Set a weak cipher to enable connection
            # https://stackoverflow.com/questions/38015537/python-requests-exceptions-sslerror-dh-key-too-small
            ftps.context.set_ciphers('DEFAULT:@SECLEVEL=1')
            ftps.connect(host, port=int(cfg.query('ena', 'ftpport', ret_default=21)), timeout=timeout)
            ftps.login(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
            ftps.prot_p()
            if self.eload not in ftps.nlst():
                self.info(f'Create {self.eload} directory')
                ftps.mkd(self.eload)
            ftps.cwd(self.eload)
            previous_content = ftps.nlst()
            for file_to_upload in files_to_upload:
                file_name = os.path.basename(file_to_upload)
                if file_name in previous_content and ftps.size(file_name) == os.path.getsize(file_to_upload):
                    self.warning(f'{file_name} Already exist and has the same size on the FTP. Skip upload.')
                    continue
                self.info(f'Upload {file_name} to FTP')
                with open(file_to_upload, 'rb') as open_file:
                    ftps.storbinary('STOR %s' % file_name, open_file)

    @retry(requests.exceptions.ConnectionError, tries=3, delay=2, backoff=1.2, jitter=(1, 3))
    def _post_xml_file_to_ena(self, url, file_dict):
        response = requests.post(
            url, auth=self.ena_auth, files=file_dict
        )
        return response

    def upload_xml_files_to_ena(self, dry_ena_upload=False):
        """Upload the xml files to the webin submission endpoint and parse the receipt."""
        submission_file, project_file, analysis_file = self.converter.create_submission_files()
        file_dict = {
            'SUBMISSION': (os.path.basename(submission_file), get_file_content(submission_file), 'application/xml'),
            'ANALYSIS': (os.path.basename(analysis_file), get_file_content(analysis_file), 'application/xml')
        }
        # If we are uploading to an existing project the project_file is not set
        if project_file:
            file_dict['PROJECT'] = (os.path.basename(project_file), get_file_content(project_file), 'application/xml')
        if dry_ena_upload:
            self.info(f'Would have uploaded the following XML files to ENA submission endpoint:')
            for key, (file_path, _, _) in file_dict.items():
                self.info(f'{key}: {file_path}')
            return
        response = self._post_xml_file_to_ena(cfg.query('ena', 'submit_url'), file_dict)
        self.results['receipt'] = response.text
        self.results.update(self.parse_ena_receipt(response.text))
        if self.results['errors']:
            self.error('\n'.join(self.results['errors']))

    def parse_ena_receipt(self, ena_xml_receipt):
        results = {'errors': []}
        try:
            receipt = ET.fromstring(ena_xml_receipt)
            message = receipt.findall('MESSAGES')[0]
            for child in message:
                if child.tag == 'ERROR':
                    results['errors'].append(child.text)
            for child in receipt:
                # Store mapping from analysis accession to analysis alias.
                if child.tag == 'ANALYSIS' and 'accession' in child.attrib:
                    results.setdefault(child.tag, {})[child.attrib['alias']] = child.attrib['accession']
                elif 'accession' in child.attrib:
                    results[child.tag] = child.attrib['accession']
        except ET.ParseError:
            self.error('Cannot parse ENA receipt: ' + ena_xml_receipt)
            results['errors'].append('Cannot parse ENA receipt: ' + ena_xml_receipt)
        return results


class ENAUploaderAsync(ENAUploader):

    def upload_xml_files_to_ena(self, dry_ena_upload=False):
        """Upload the xml file to the asynchronous endpoint and monitor the results from the poll endpoint."""

        webin_file = self.converter.create_single_submission_file()
        file_dict = {
            'file': (os.path.basename(webin_file), get_file_content(webin_file), 'application/xml'),
        }
        if dry_ena_upload:
            self.info(f'Would have uploaded the following XML files to ENA asynchronous submission endpoint:')
            for key, (file_path, _, _) in file_dict.items():
                self.info(f'{key}: {file_path}')
            return
        response = self._post_xml_file_to_ena(cfg.query('ena', 'submit_async'), file_dict)
        json_data = response.json()
        if 'submissionId' in json_data:
            xml_link = [link_dict['href'] for link_dict in json_data['links'] if link_dict['rel'] == 'poll-xml'][0]
            self.results['submissionId'] = json_data['submissionId']
            self.results['poll-links'] = xml_link
            self.monitor_results()
        else:
            self.results['errors'] = [f'{json_data["status"]}: {json_data["error"]} - {json_data["message"]}']

    def monitor_results(self, timeout=3600, wait_time=30):
        xml_link = self.results['poll-links']
        response = requests.get(xml_link, auth=self.ena_auth)
        time_lapsed = 0
        while response.status_code == 202:
            if time_lapsed > timeout:
                raise TimeoutError(f'Waiting for ENA receipt for more than {timeout} seconds')
            self.info(f'Waiting {wait_time} for submission to ENA to be processed')
            time.sleep(wait_time)
            time_lapsed += wait_time

            response = requests.get(xml_link, auth=self.ena_auth)
        self.parse_ena_receipt(response.text)
        self.results.update(self.parse_ena_receipt(response.text))
        if self.results['errors']:
            self.error('\n'.join(self.results['errors']))

