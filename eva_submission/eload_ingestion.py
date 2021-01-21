import string
import subprocess

from bs4 import BeautifulSoup
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.mongo_utils import get_mongo_connection_handle
import requests

from eva_submission.eload_submission import Eload


def sanitize(s):
    """Lowercases and removes punctuation from s, e.g. to make it a legal MongoDB identifier."""
    return s.lower().translate(str.maketrans('', '', string.punctuation))


class EloadIngestion(Eload):
    config_section = 'ingestion'  # top-level config key
    all_tasks = ['metadata_load', 'accession', 'variant_load']

    def __init__(self, eload_number):
        super().__init__(eload_number)
        self.eload_cfg.set(self.config_section, 'ingestion_date', value=self.now)

    def ingest(self, db_name=None, tasks=None):
        # TODO assembly/taxonomy insertion script should be incorporated here
        self.check_variant_db(db_name)

        if not tasks:
            tasks = self.all_tasks

        if 'metadata_load' in tasks:
            self.load_from_ena()
        if 'accession' in tasks:
            self.warning('Accessioning not yet supported, skipping.')
        if 'variant_load' in tasks:
            self.warning('Variant loading not yet supported, skipping.')

    def get_db_name(self):
        """
        Constructs the expected database name in mongo, based on assembly info retrieved from ENA.
        """
        assm_accession = self.eload_cfg.query('submission', 'assembly_accession')
        ena_url = f'https://www.ebi.ac.uk/ena/browser/api/xml/{assm_accession}'
        try:  # catches any kind of request error, including non-20X status code
            response = requests.get(ena_url)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.error(f"Couldn't get assembly info from ENA for accession {assm_accession}")
            raise e

        soup = BeautifulSoup(response.text, 'lxml')
        sci_name_terms = soup.scientific_name.text.split()
        sci_name = sci_name_terms[0][0] + ''.join(sci_name_terms[1:])
        assembly_name = soup.find('name').text
        return f'eva_{sanitize(sci_name)}_{sanitize(assembly_name)}'

    def check_variant_db(self, db_name=None):
        """
        Checks mongo for the right variant database.
        If db_name is provided it will check for that, otherwise it will construct the expected database name.
        """
        #
        if not db_name:
            db_name = self.get_db_name()
        self.eload_cfg.set(self.config_section, 'database', 'db_name', value=db_name)

        with get_mongo_connection_handle(
                username=cfg.query('mongo', 'username'),
                password=cfg.query('mongo', 'password'),
                host=cfg.query('mongo', 'host')
        ) as db:
            names = db.list_database_names()
            if db_name in names:
                self.info(f'Found database named {db_name}.')
                self.info('If this is incorrect, please cancel and pass in the database name explicitly.')
                self.eload_cfg.set(self.config_section, 'database', 'exists', value=True)
            else:
                self.error(f'Database named {db_name} does not exist in variant warehouse, aborting.')
                self.error('Please create the database or pass in the appropriate database name explicitly.')
                self.eload_cfg.set(self.config_section, 'database', 'exists', value=False)
                raise ValueError(f'No database named {db_name} found.')

    def load_from_ena(self):
        """
        Loads project metadata from ENA into EVADEV.
        """
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        if not project_accession:
            self.error('No project accession in submission config, check that brokering to ENA is done. ')
            raise ValueError('No project accession in submission config.')
        try:
            command_utils.run_command_with_output(
                'Load metadata from ENA to EVADEV',
                ' '.join((
                    'perl', cfg['executable']['load_from_ena'],
                    '-p', project_accession,
                    # Current submission process never changes -c or -v
                    '-c', 'submitted',
                    '-v', '1',
                    # -l is only checked for when -c=eva_value_added, so in reality never used
                    '-l', self._get_dir('scratch'),
                    '-e', str(self.eload_num)
                ))
            )
            self.eload_cfg.set(self.config_section, 'ena_load', value='success')
        except subprocess.CalledProcessError as e:
            self.error('ENA metadata load failed: aborting ingestion.')
            self.eload_cfg.set(self.config_section, 'ena_load', value='failure')
            raise e
