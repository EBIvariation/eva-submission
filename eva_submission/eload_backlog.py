from cached_property import cached_property

from ebi_eva_common_pyutils.pg_utils import get_all_results_for_query

from eva_submission.eload_submission import Eload
from eva_submission.eload_utils import get_metadata_conn


class EloadBacklog(Eload):

    def fill_in_params(self):
        """Fills in config params from metadata DB and ENA, enabling later parts of pipeline to run."""
        self.eload_cfg.set('brokering', 'ena', 'PROJECT', value=self.project_accession)
        self.get_analysis_info()

    @cached_property
    def project_accession(self):
        with get_metadata_conn() as conn:
            query = f"select project_accession from evapro.project_eva_submission where eload_id={self.eload_num};"
            rows = get_all_results_for_query(conn, query)
            if len(rows) != 1:
                raise ValueError(f'No project accession for {self.eload} found in metadata DB.')
            return rows[0][0]

    def get_analysis_info(self):
        with get_metadata_conn() as conn:
            query = f"select a.analysis_accession, array_agg(c.filename) " \
                    f"from project_analysis a " \
                    f"join analysis_file b on a.analysis_accession=b.analysis_accession " \
                    f"join file c on b.file_id=c.file_id " \
                    f"where a.project_accession='{self.project_accession}' " \
                    f"group by a.analysis_accession;"
            rows = get_all_results_for_query(conn, query)
            if len(rows) == 1:
                raise ValueError(f'No analyses for {self.eload} found in metadata DB.')
            for analysis_accession, filenames in rows:
                # TODO figure out which are indexes and which are vcfs
                #  - get the full file paths by combining this with known eload directory
                pass
