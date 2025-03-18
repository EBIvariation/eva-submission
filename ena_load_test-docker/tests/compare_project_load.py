import datetime
import os.path
import time
import unittest

import ebi_eva_internal_pyutils
import psycopg2
import pytest
from ebi_eva_common_pyutils.command_utils import run_command_with_output
from ebi_eva_common_pyutils.config import cfg

import eva_sub_cli_processing
from eva_submission.evapro.populate_evapro import EnaProjectFinder, EvaProjectLoader
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
        projects = ['PRJEB31129', 'PRJEB82556', 'PRJEB58296', 'PRJEB61107']

        for idx, project in enumerate(projects):
            eload = idx + 3
            self.load_through_perl(project, eload)
            try:
                self.loader.load_project_from_ena(project, eload)
            except Exception as e:
                print(f'Failed to load project {project} with python script: {e}')
        # All the tables that the perl script references
        for table_name in tables_to_tests:
            query = f'select * from {table_name}'
            report = self._compare_perl_python_with_query(query)
            if report:
                print(f'===== Error while Testing {table_name} content =====')
                print('\n'.join(report))

    @pytest.mark.skip(reason='Not now')
    def test_project_same_in_perl_and_python_load(self):
        '''Assess the semantic linking between the project and the other tables'''

        # Load the project with python / sqlalchemy
        self.loader.load_project_from_ena(self.project_to_load, self.eload)

        print('===== Test the project table =====')
        fields = ['p.' + v for v in [
            'project_accession', 'center_name', 'alias', 'title', 'description', 'scope', 'material', 'selection',
            'type',
            'secondary_study_id', 'hold_date', 'source_type', 'eva_description', 'eva_center_name',
            'eva_submitter_link', 'ena_status', 'eva_status', 'ena_timestamp', 'eva_timestamp',
            'study_type'
        ]]
        # MISSING fields: project_accession_code and eva_study_accession are not being set in the python script
        where_clause = f"p.project_accession = '{self.project_to_load}'"
        self._compare_perl_python_in_table(fields=fields, table='project p', where_clause=where_clause)

        print('===== Test the analysis table linked through project analysis =====')
        fields = ['a.' + v for v in [
            'analysis_accession','title','alias','description','center_name','date','vcf_reference',
            'vcf_reference_accession','hidden_in_eva','assembly_set_id'
        ]]
        from_and_join = (
            'project p join project_analysis pa on p.project_accession=pa.project_accession '
            'join analysis a on pa.analysis_accession=a.analysis_accession'
        )
        self._compare_perl_python_in_table(fields=fields, table=from_and_join, where_clause=where_clause)
        # MISSING fields: assembly_set_id is not being set in the perl script
        # MISSING fields: date are not being set the same way

        print('===== Test the file table linked through project_analysis + analysis_file =====')
        fields = ['f.' + v for v in [
            'file_id','ena_submission_file_id','filename','file_md5','file_location','file_type','file_class',
            'file_version','is_current','ftp_file','mongo_load_status','eva_submission_file_id '
        ]]
        from_and_join = (
            'project p join project_analysis pa on p.project_accession=pa.project_accession '
            'join analysis a on pa.analysis_accession=a.analysis_accession '
            'join analysis_file af on a.analysis_accession=af.analysis_accession '
            'join file f on af.file_id=f.file_id'
        )
        self._compare_perl_python_in_table(fields=fields, table=from_and_join, where_clause=where_clause)

        print('===== Test the assembly_set table linked through project_analysis + assembly_set =====')
        fields = ['aset.' + v for v in ['assembly_set_id','taxonomy_id','assembly_name','assembly_code']]
        from_and_join = (
            'project p join project_analysis pa on p.project_accession=pa.project_accession '
            'join analysis a on pa.analysis_accession=a.analysis_accession '
            'join analysis_file af on a.analysis_accession=af.analysis_accession '
            'join assembly_set aset on a.assembly_set_id=aset.assembly_set_id'
        )
        self._compare_perl_python_in_table(fields=fields, table=from_and_join, where_clause=where_clause)

        print('===== Test the taxonomy table linked through project_taxonomy =====')
        fields = ['t.' + v for v in ['taxonomy_id','common_name','scientific_name','taxonomy_code','eva_name ']]
        from_and_join = (
            'project p join project_taxonomy pt on p.project_accession=pt.project_accession '
            'join taxonomy t on pt.taxonomy_id=t.taxonomy_id'
        )
        self._compare_perl_python_in_table(fields=fields, table=from_and_join, where_clause=where_clause)

        print('===== Test the submission table linked through project_ena_submission =====')
        fields = ['sub.' + v for v in ['submission_id','submission_accession','type','action','title','notes','date','brokered']]
        from_and_join = (
            'project p join project_ena_submission pes on p.project_accession=pes.project_accession '
            'join submission sub on pes.submission_id=sub.submission_id'
        )
        self._compare_perl_python_in_table(fields=fields, table=from_and_join, where_clause=where_clause)

    def _difference_in_query_results(self, list1, list2, header_list=None):
        report = []
        for idx1 in range(max(len(list1), len(list2))):
            entry1 = None
            entry2 = None
            if idx1 < len(list1):
                entry1 = list1[idx1]
            else:
                report.append(f'List 1 is missing value {idx1 + 1} matching {list2[idx1]}')
            if idx1 < len(list2):
                entry2 = list2[idx1]
            else:
                report.append(f'List 2 is missing value {idx1 + 1} matching {list1[idx1]}')
            if entry1 and entry2:
                if not header_list:
                    header_list = [f'field {i}' for i in range(max(len(entry1), len(entry2)))]
                for idx2, header in enumerate(header_list):
                    if entry1[idx2] != entry2[idx2]:
                        report.append(f'{header}: {entry1[idx2]} != {entry2[idx2]}')
                    else:
                        # print(f'{header}: {entry1[idx2]} ==> {entry2[idx2]}')
                        pass
        return report

    def _compare_perl_python_in_table(self, fields, table, where_clause=None):
        query = f"select {','.join(fields)} from {table}"
        if where_clause:
            query += f" where {where_clause};"
        else:
            query += f";"
        print(query)
        with self._connect_python_postgresql() as python_cursor:
            python_cursor.execute(query)
            results_python = python_cursor.fetchall()

        with self._connect_perl_postgresql() as python_cursor:
            python_cursor.execute(query)
            results_perl = python_cursor.fetchall()
        return not self._difference_in_query_results(results_perl, results_python, fields)

    def _compare_perl_python_with_query(self, query):
        report = []
        with self._connect_python_postgresql() as python_cursor:
            try:
                python_cursor.execute(query)
                results_python = python_cursor.fetchall()
            except psycopg2.Error as e:
                return ['In python docker -- Error:' + str(e)]
        with self._connect_perl_postgresql() as python_cursor:
            try:
                python_cursor.execute(query)
                results_perl = python_cursor.fetchall()
            except psycopg2.Error as e:
                return ['In perl docker -- Error:' + str(e)]
        return self._difference_in_query_results(results_perl, results_python)

    def _connect_perl_postgresql(self):
        pg_conn = psycopg2.connect(host='localhost', port=5433, dbname='metadata', user='root_user', password='root_pass')
        pg_cursor = pg_conn.cursor()
        return pg_cursor

    def _connect_python_postgresql(self):
        pg_conn = psycopg2.connect(host='localhost', port=5432, dbname='metadata', user='root_user', password='root_pass')
        pg_cursor = pg_conn.cursor()
        return pg_cursor