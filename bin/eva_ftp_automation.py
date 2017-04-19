#!/usr/bin/python

import argparse
import csv
import psycopg2
import getpass

parser = argparse.ArgumentParser(description='This script pulls a CSV list of files to be shared'
                                             'from the EVA FTP for a particular project, '
                                             'split by analysis accession and also gives the'
                                             'associated md5sum value for each file.')
parser.add_argument('-p','--project_id', help='project_id to pull files from ERAPRO',
                    required=True, dest='project_id')
parser.add_argument('-f','--outputdest', help='output file from running script', required=True,
                    dest='dest')
parser.add_argument('-d','--db', help='database to connect to', required=True, dest='database')
parser.add_argument('-u','--dbuname', help='username for database connection', required=True,
                    dest='user')
parser.add_argument('-H','--hostname', help='host for database connection', required=True,
                    dest='host')
parser.add_argument('-P','--dbport', help='port for database connection', required=True,
                    dest='port')
args = parser.parse_args()

my_pwd = getpass.getpass(stream=None)
conn = psycopg2.connect(database=args.database, user=args.user, password=my_pwd, host=args.host,
                        port=args.port)
cur = conn.cursor()

cur.execute("""SELECT project_analysis.project_accession, analysis.analysis_accession, 
        file.filename, file.file_md5, file.file_location
        FROM project_analysis
        LEFT JOIN analysis on project_analysis.analysis_accession = analysis.analysis_accession
        LEFT JOIN analysis_file on analysis.analysis_accession = analysis_file.analysis_accession
        LEFT JOIN file on analysis_file.file_id = file.file_id
        WHERE project_accession = %s and analysis.hidden_in_eva = 0;""", (args.project_id,))

records = cur.fetchall()

with (open(args.dest, 'w')) as f:
    writer = csv.writer (f, delimiter = ',')
    for row in records:
        writer.writerow(row)

conn.close()