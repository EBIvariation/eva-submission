#!/usr/bin/env nextflow


nextflow.enable.dsl=2


/*
 * Copy files from eva_public to FTP folder.
 */
 process copy_to_ftp {
    label 'datamover', 'short_time', 'small_mem'

    input:
    // ensures that all indices are done before we copy
    file csi_indices
    val accessioned_vcfs

    script:
    if( accessioned_vcfs.size() > 0 )
        """
        set -eo pipefail
        cd $params.public_dir
        # remove the uncompressed accessioned vcf file
        rm -f ${accessioned_vcfs.join(' ')}
        rsync -va * ${params.public_ftp_dir}/${params.project_accession}
        ls -l ${params.public_ftp_dir}/${params.project_accession}/*
        """
    else
        """
        set -eo pipefail
        cd $params.public_dir
        rsync -va * ${params.public_ftp_dir}/${params.project_accession}
        ls -l ${params.public_ftp_dir}/${params.project_accession}/*
        """
}