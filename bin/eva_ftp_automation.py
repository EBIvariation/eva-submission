#!/usr/bin/python

import argparse
import psycopg2
import csv

parser = argparse.ArgumentParser(description='insert the project ID as an argument')
parser.add_argument('-p','--project_id', help='project_id to pull files from ERAPRO', required=True, dest='project_id')
args = parser.parse_args()
pid = args.project_id

conn = psycopg2.connect(database="", user="", password="", host="", port="")
cur = conn.cursor()

cur.execute("""SELECT project_analysis.project_accession, analysis.analysis_accession, file.filename, file.file_md5, file.file_location
        FROM project_analysis
        LEFT JOIN analysis on project_analysis.analysis_accession = analysis.analysis_accession
        LEFT JOIN analysis_file on analysis.analysis_accession = analysis_file.analysis_accession
        LEFT JOIN file on analysis_file.file_id = file.file_id
        WHERE project_accession = %s and analysis.hidden_in_eva = '0';""", (args.project_id,))

records = cur.fetchall()

with (open('/nfs/production3/eva/user/gary/evapro_ftp/%s.csv' % (args.project_id,), 'w')
      ) as f:
    writer = csv.writer (f, delimiter = ',')
    for row in records:
        writer.writerow(row)

conn.close()
