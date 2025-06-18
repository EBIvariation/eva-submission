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
    parser.add_argument("--shallow", required=False, action="store_true")
    args = parser.parse_args()
    os.mkdir(os.path.join(args.submission_dir, 'validation_output'))
    os.mkdir(os.path.join(args.submission_dir, 'validation_output/assembly_check'))
    os.mkdir(os.path.join(args.submission_dir, 'validation_output/vcf_format'))
    touch(os.path.join(args.submission_dir, 'validation_output/report.html'))
    touch(os.path.join(args.submission_dir, 'validation_output/report.txt'))
    touch(os.path.join(args.submission_dir, 'validation_results.yaml'))
    touch(os.path.join(args.submission_dir, 'validation_output/assembly_check/test1.vcf.gz.text_assembly_report1234.txt'))
    touch(os.path.join(args.submission_dir, 'validation_output/assembly_check/test1.vcf.gz.assembly_check.log'))
    touch(os.path.join(args.submission_dir, 'validation_output/vcf_format/test1.vcf.gz.errors.1234.txt'))
    touch(os.path.join(args.submission_dir, 'validation_output/vcf_format/test1.vcf.gz.vcf_format.log'))
