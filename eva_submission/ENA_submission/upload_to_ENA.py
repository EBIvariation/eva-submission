import ftplib
import json
import os
import time
from xml.etree import ElementTree as ET

import requests
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.config import cfg
from requests.auth import HTTPBasicAuth
from retry import retry

from eva_submission.ENA_submission.json_to_ENA_json import EnaJsonConverter
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
    def __init__(self, submission_id, metadata_file, output_dir):
        self.submission_id = submission_id
        self.results = {}
        if metadata_file.endswith('.xlsx'):
            self.converter = EnaXlsxConverter(self.submission_id, metadata_file, output_dir, self.submission_id)
        elif metadata_file.endswith('.json'):
            self.converter = EnaJsonConverter(submission_id, metadata_file, output_dir, 'ENA_submission')
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
            if self.submission_id not in ftps.nlst():
                self.info(f'Create {self.submission_id} directory')
                ftps.mkd(self.submission_id)
            ftps.cwd(self.submission_id)
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
    def _post_metadata_file_to_ena(self, url, file_dict, mime_type='application/json'):
        response = requests.post(
            url, auth=self.ena_auth, files=file_dict, headers={"Accept": mime_type}
        )
        return response

    def upload_metadata_file_to_ena(self, dry_ena_upload=False):
        """Upload the xml file to the webin submission endpoint and parse the receipt."""
        webin_file = self.converter.create_single_submission_file()
        mime_type = 'application/xml'
        if webin_file.endswith('.json'):
            mime_type = 'application/json'

        file_dict = {
            'file': (os.path.basename(webin_file), get_file_content(webin_file), mime_type),
        }
        if dry_ena_upload:
            self.info(f'Would have uploaded the following files to ENA submission endpoint:')
            for key, (file_path, _, _) in file_dict.items():
                self.info(f'{key}: {file_path}')
            return
        response = self._post_metadata_file_to_ena(cfg.query('ena', 'submit_url'), file_dict, mime_type)
        self.results['receipt'] = response.text
        if response.headers['Content-Type'] == 'application/xml':
            self.results.update(self.parse_ena_xml_receipt(response.text))
        else:
            self.results.update(self.parse_ena_json_receipt(response.text))
        if self.results['errors']:
            self.error('\n'.join(self.results['errors']))

    def parse_ena_xml_receipt(self, ena_xml_receipt):
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

    def parse_ena_json_receipt(self, ena_json_receipt):
        results = {'errors': []}
        try:
            receipt = json.loads(ena_json_receipt)
            messages = receipt.get('messages')
            if "error" in messages:
                results['errors'].extend(messages['error'])
            for key in receipt:
                if key == 'analyses':
                    results['ANALYSIS'] = {a['alias']: a['accession'] for a in receipt[key]}
                elif key == 'projects':
                    results['PROJECT'] = receipt[key][0]['accession']
                elif key == 'submission':
                    results['SUBMISSION'] = receipt[key]['accession']
        except Exception as e:
            self.error('Cannot parse ENA json receipt: ' + ena_json_receipt)
            results['errors'].append('Cannot parse ENA json receipt: ' + ena_json_receipt)
        return results


class ENAUploaderAsync(ENAUploader):

    def upload_metadata_file_to_ena(self, dry_ena_upload=False):
        """Upload the xml file to the asynchronous endpoint and monitor the results from the poll endpoint."""

        webin_file = self.converter.create_single_submission_file()
        mime_type = 'application/xml'
        if webin_file.endswith('.json'):
            mime_type = 'application/json'
        file_dict = {
            'file': (os.path.basename(webin_file), get_file_content(webin_file), mime_type),
        }
        if dry_ena_upload:
            self.info(f'Would have uploaded the following metadata files to ENA asynchronous submission endpoint:')
            for key, (file_path, _, _) in file_dict.items():
                self.info(f'{key}: {file_path}')
            return
        response = self._post_metadata_file_to_ena(cfg.query('ena', 'submit_async'), file_dict, mime_type)
        if response.status_code == 200:
            json_data = response.json()
            self.results['submissionId'] = json_data.get('submissionId')
            self.results['poll-links'] = json_data.get('_links').get('poll').get('href')
            if self.results['submissionId'] and self.results['poll-links']:
                self.monitor_results()
            else:
                self.results['errors'] = [f'No links present in json document: {json_data}']
        else:
            self.results['errors'] = [f'{response.status_code}']

    def monitor_results(self, timeout=3600, wait_time=30):
        poll_link = self.results['poll-links']
        response = requests.get(poll_link, auth=self.ena_auth, headers={"Accept": "application/json"})
        time_lapsed = 0
        while response.status_code == 202:
            self.debug(f'{poll_link} -> {response.status_code} : {response.text}')
            if time_lapsed > timeout:
                self.error(f'Timed out waiting for {poll_link}')
                raise TimeoutError(f'Waiting for ENA receipt from {poll_link} for more than {timeout} seconds')
            self.info(f'Waiting {wait_time} for submission to ENA to be processed')
            time.sleep(wait_time)
            time_lapsed += wait_time

            response = requests.get(poll_link, auth=self.ena_auth, headers={"Accept": "application/json"})
        self.results.update(self.parse_ena_json_receipt(response.text))
        if self.results['errors']:
            self.error('\n'.join(self.results['errors']))

