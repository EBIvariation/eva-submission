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
    parser.add_argument("--shallow", required=True, action="store_true")
    args = parser.parse_args()
    os.mkdir(os.path.join(args.submission_dir, 'validation_output'))
    touch(os.path.join(args.submission_dir, 'validation_output/report.html'))
    touch(os.path.join(args.submission_dir, 'validation_results.yaml'))
