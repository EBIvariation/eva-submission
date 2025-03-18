import datetime
import os.path
import time
import unittest

import psycopg2
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.common_utils import pretty_print
from ebi_eva_common_pyutils.config import cfg

from eva_submission.evapro.populate_evapro import EvaProjectLoader
from eva_submission.submission_config import load_config


# @pytest.mark.skip(reason='Needs access to ERA database')
class TestCompareProjectLoad(unittest.TestCase):
    compose_dir = os.path.dirname(os.path.dirname(__file__))
    project_to_load = 'PRJEB31129'
    eload = '3'

    @classmethod
    def setUpClass(cls):
        os.chdir(cls.compose_dir)
        command = 'docker compose up -d --build'
        run_command_with_output('Start docker-compose in the background', command)
        # Wait for the postgres to be ready to accept command
        time.sleep(10)

    @classmethod
    def tearDownClass(cls):
        os.chdir(cls.compose_dir)
        command = 'docker compose down'
        run_command_with_output('Stop docker-compose running in the background', command)

    def setUp(self):
        config_file = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))),
                                   'tests', 'resources', 'erapro_config.yaml')
        maven_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'maven-settings.xml')
        if os.path.exists(config_file):
            load_config(config_file)
            cfg.content['maven'] = {'environment':'docker', 'settings_file': maven_file}
        else:
            print(f'Config file {config_file} is not present. Add the config file to run the tests using ERA')
        self.loader = EvaProjectLoader()

    def build_table_2_fields(self, table_names):
        table_2_fields = {}
        query = "SELECT column_name FROM information_schema.columns WHERE table_schema = 'evapro' AND table_name='{}' order by ordinal_position"
        with self._connect_python_postgresql() as python_cursor:
            for table_name in table_names:
                python_cursor.execute(query.format(table_name))
                results = [field[0] for field in python_cursor.fetchall()]
                table_2_fields[table_name] = results
        return table_2_fields

    def load_through_perl(self, project_accession, eload):
        command = ('docker exec ena_perl_loader perl /usr/local/software/modified_load_from_ena_postgres_or_file.pl '
                   f'-c submitted -p {project_accession} -v 1 -e {eload} ')
        run_command_with_output(f'Load project {project_accession} with perl', command)

    def test_all_tables_contents(self):
        '''Assess All tables regardless of their contents and links'''
        tables_to_tests = [
            'analysis','analysis_experiment_type','analysis_file','analysis_platform',
            'analysis_sequence','analysis_submission','assembly_set','browsable_file',
            'custom_assembly','dbxref','eva_submission',
            'experiment_type','file','linked_project','project','project_analysis','project_dbxref',
            'project_ena_submission','project_eva_submission','project_samples_temp1',
            'project_taxonomy','submission','taxonomy'
        ]
        table_2_fields = self.build_table_2_fields(tables_to_tests)

        projects = ['PRJEB5829']

        for idx, project in enumerate(projects):
            eload = idx + 3
            try:
                self.load_through_perl(project, eload)
            except Exception as e:
                print(f'Failed to load project {project} with perl script: {e}')
            try:
                self.loader.load_project_from_ena(project, eload)
            except Exception as e:
                print(f'Failed to load project {project} with python script: {e}')
        # All the tables that the perl script references
        for table_name in tables_to_tests:
            query = f'select * from {table_name}'
            report, differences = self._compare_perl_python_with_query(query, fields=table_2_fields[table_name])
            if differences:
                print(f'===== Difference found while Testing {table_name} content =====')
                if isinstance(report[0], list):
                    pretty_print(table_2_fields[table_name], report)
                else:
                    print('\n'.join(report))

    def _difference_in_query_results(self, list1, list2, header_list):
        report = []
        difference = False
        for idx1 in range(max(len(list1), len(list2))):
            if idx1 < len(list1):
                entry1 = list1[idx1]
            else:
                entry1 = ['Empty'] * len(header_list)
            if idx1 < len(list2):
                entry2 = list2[idx1]
            else:
                entry2 = ['Empty'] * len(header_list)
            row = []
            for idx2, header in enumerate(header_list):
                if entry1[idx2] != entry2[idx2]:
                    row.append(str(entry1[idx2])+'!='+str(entry2[idx2]))
                    difference = True
                else:
                    row.append('==')
            report.append(row)
        return report, difference

    def _compare_perl_python_with_query(self, query, fields=None):
        report = []
        difference = False
        with self._connect_python_postgresql() as python_cursor:
            try:
                python_cursor.execute(query)
                results_python = python_cursor.fetchall()
            except psycopg2.Error as e:
                return ['In python docker -- Error:' + str(e)], True
        with self._connect_perl_postgresql() as python_cursor:
            try:
                python_cursor.execute(query)
                results_perl = python_cursor.fetchall()
            except psycopg2.Error as e:
                return ['In perl docker -- Error:' + str(e)], True
        return self._difference_in_query_results(results_perl, results_python, fields)

    def _connect_perl_postgresql(self):
        pg_conn = psycopg2.connect(host='localhost', port=5433, dbname='metadata', user='root_user', password='root_pass')
        pg_cursor = pg_conn.cursor()
        return pg_cursor

    def _connect_python_postgresql(self):
        pg_conn = psycopg2.connect(host='localhost', port=5432, dbname='metadata', user='root_user', password='root_pass')
        pg_cursor = pg_conn.cursor()
        return pg_cursor