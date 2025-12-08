#!/usr/bin/env nextflow


nextflow.enable.dsl=2

/*
 * Chunk VCF into 10 parts per genome
 */
process chunk_vcf {
    label 'default_time', 'med_mem'

    input:
    tuple val(vcf_filename), val(vcf_file), val(assembly_accession), val(aggregation), val(fasta), val(report), val(num_chunks)

    output:
    tuple val(vcf_filename), path("chunks/*.vcf"), val(assembly_accession), val(aggregation), val(fasta), val(report), emit: chunked_vcfs

    script:
    """
    set -euo pipefail

    mkdir chunks

    # Get chromosome list from VCF
    $params.executable.bcftools query -f '%CHROM\n' $vcf_file | sort -u > chroms.txt

    # Create window BED with ${num_chunks} windows per chromosome
    > windows.bed
    while read CHR; do
        # Get chromosome length using samtools faidx
        LEN=\$($params.executable.samtools faidx ${fasta} \$CHR | grep -v ">" | wc -c)
        WINDOW_SIZE=\$(( (LEN + ${num_chunks} - 1) / ${num_chunks} ))

        START=1
        for ((i=1; i<=${num_chunks}; i++)); do
            END=\$(( START + WINDOW_SIZE - 1 ))
            if [ \$START -le \$LEN ]; then
                if [ \$END -gt \$LEN ]; then END=\$LEN; fi
                echo -e "\$CHR\t\$START\t\$END" >> windows.bed
            fi
            START=\$(( END + 1 ))
        done
    done < chroms.txt

    # Create per-chunk VCFs
    CHUNK_ID=0
    BASE_NAME="${vcf_filename.replaceAll(/\.vcf(\.gz)?$/, '')}"
    while read -r CHR S E; do
        CHUNK_ID=\$((CHUNK_ID+1))
        OUT="chunks/\${BASE_NAME}.chunk\$CHUNK_ID.vcf"

        $params.executable.bcftools view -r \$CHR:\$S-\$E -O v -o \$OUT ${vcf_file} || true

        # Remove empty chunks
        if [ ! -s \$OUT ] || $params.executable.bcftools view -H \$OUT | wc -l | grep -q '^0'; then
            rm -f \$OUT
            continue
        fi

    done < windows.bed
    """
}