import ftplib
import os
from xml.etree import ElementTree as ET

import requests
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.config import cfg
from requests.auth import HTTPBasicAuth

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
    def __init__(self, eload):
        self.eload = eload
        self.results = {}

    def upload_vcf_files_to_ena_ftp(self, files_to_upload):
        self.info('Connect to %s', cfg.query('ena', 'ftphost'))
        ftps = HackFTP_TLS()
        host = cfg.query('ena', 'ftphost')
        ftps.connect(host, port=int(cfg.query('ena', 'ftpport', ret_default=21)))
        ftps.login(cfg.query('ena', 'username'), cfg.query('ena', 'password'))
        ftps.prot_p()
        if self.eload not in ftps.nlst():
            self.info('Create %s directory' % self.eload)
            ftps.mkd(self.eload)
        ftps.cwd(self.eload)
        for file_to_upload in files_to_upload:
            file_name = os.path.basename(file_to_upload)
            self.info('Upload %s to FTP' % file_name)
            with open(file_to_upload, 'rb') as open_file:
                ftps.storbinary('STOR %s' % file_name, open_file)

    def upload_xml_files_to_ena(self, submission_file, project_file, analysis_file):
        file_dict = {
            'SUBMISSION': (os.path.basename(submission_file), get_file_content(submission_file), 'application/xml'),
            'ANALYSIS': (os.path.basename(analysis_file), get_file_content(analysis_file), 'application/xml')
        }
        # If we are uploading to an existing project the project_file is not set
        if project_file:
            file_dict['PROJECT'] = (os.path.basename(project_file), get_file_content(project_file), 'application/xml')

        response = requests.post(
            cfg.query('ena', 'submit_url'),
            auth=HTTPBasicAuth(cfg.query('ena', 'username'), cfg.query('ena', 'password')),
            files=file_dict
        )
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
