#!/usr/bin/env nextflow

def helpMessage() {
    log.info"""
    Migrate ELOAD (and optionally project) to codon

    Inputs:
            --eload                 ELOAD to migrate
            --project_accession     Project accession (optional)
            --old_submissions_dir   Old location of submissions
            --new_submissions_dir   New location of submissions
            --old_projects_dir      Old location of projects
            --new_projects_dir      New location of projects
    """
}

params.eload = null
params.project_accession = null
params.old_submissions_dir = null
params.new_submissions_dir = null
params.old_projects_dir = null
params.new_projects_dir = null
params.help = null

// Show help message
if (params.help) exit 0, helpMessage()

// Test input files
if (!params.eload || !params.old_submissions_dir || !params.new_submissions_dir || !params.old_projects_dir || !params.new_projects_dir) {
    exit 1, helpMessage()
}


process copy_submission_dir {
    label 'datamover'

    script:
    """
    rsync -rlA ${params.old_submissions_dir}/${params.eload} ${params.new_submissions_dir}/${params.eload}
    """
}


process copy_project_dir {
    label 'datamover'

    when:
    params.project_accession != null

    script:
    """
    rsync -rlA ${params.old_projects_dir}/${params.project_accession} ${params.new_projects_dir}/${params.project_accession}
    """
}
