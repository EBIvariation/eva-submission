from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg

from eva_submission.eload_submission import Eload


class EloadIngestion(Eload):

    def __init__(self, eload_number: int):
        super().__init__(eload_number)

    def load_from_ena(self):
        # TODO I think project accession is only really necessary on this step
        project_accession = self.eload_cfg.query('brokering', 'ena', 'PROJECT')
        if not project_accession:
            self.error('No project accession in submission config, check that brokering to ENA is done.')
            raise ValueError('No project accession in submission config')
        command_utils.run_command_with_output(
            'Load metadata from ENA to EVADEV',
            ' '.join((
                'perl', cfg['executable']['load_from_ena'],
                '-p', project_accession,
                # Current submission process never changes -c or -v
                '-c', 'submitted',
                '-v', "1",
                '-l', self._get_dir('scratch'),  # TODO does this need to be 20_scratch/eva?
                '-e', str(self.eload_num)
            ))
        )
