#!/usr/bin/env python
import argparse
import os


def touch(f):
    open(f, 'w').close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--submission_dir", required=True)
    parser.add_argument("--metadata_json", required=True)
    parser.add_argument("--tasks", required=True)
    parser.add_argument("--validation_tasks", nargs="+", required=True)
    parser.add_argument("--shallow", required=False, action="store_true")
    args = parser.parse_args()
    os.mkdir(os.path.join(args.submission_dir, 'validation_output'))
    touch(os.path.join(args.submission_dir, 'validation_output/report.html'))
    touch(os.path.join(args.submission_dir, 'validation_output/report.txt'))
    touch(os.path.join(args.submission_dir, 'validation_results.yaml'))
