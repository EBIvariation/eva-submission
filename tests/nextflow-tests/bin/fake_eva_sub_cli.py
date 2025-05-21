#!/usr/bin/env python
import argparse
import os


def touch(f):
    open(f, 'w').close()

'''
--submission_dir . --metadata_json ${params.metadata_json} --tasks VALIDATE
'''

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--submission_dir", required=True)
    parser.add_argument("--metadata_json", required=True)
    parser.add_argument("--tasks", required=True)
    args = parser.parse_args()
    touch(os.path.join(args.submission_dir, 'report.html'))
    touch(os.path.join(args.submission_dir, 'validation_results.yaml'))
