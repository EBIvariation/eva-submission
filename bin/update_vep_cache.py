#!/usr/bin/python

import argparse
import string
from ftplib import FTP, error_perm

parser = argparse.ArgumentParser(description='This script downloads and decompresses a list of caches from ensemble and '
                                             'ensembl genomes. The required parameters are the release versions of both '
                                             'ensembl and ensembl genomes. It is recommended to run first with -t to '
                                             'check that all files exist.')
parser.add_argument('-e', '--ensembl-version', help='release version of the ensembl cache',
                    required=True, dest='ensembl_version')
parser.add_argument('-g', '--ensembl-genomes-version', help='release version of the ensembl genomes cache',
                    required=True, dest='ensembl_genomes_version')
parser.add_argument('-t', '--test', help='check that all the files exist in the remote ftp',
                    required=False, dest='test', action='store_true')
args = parser.parse_args()


# the ensembl link structure is like:
# fasta: ftp://ftp.ensembl.org/pub/release-89/fasta/bos_taurus/dna/Bos_taurus.UMD3.1.dna.toplevel.fa.gz
# cache: ftp://ftp.ensembl.org/pub/release-89/variation/VEP/bos_taurus_vep_89_UMD3.1.tar.gz
# so this array is:
# ['species.assembly', ...]
species_ensembl = [
    'Bos_taurus.UMD3.1',
    'Chlorocebus_sabaeus.ChlSab1.1',
    'Mus_musculus.GRCm38',
    'Ovis_aries.Oar_v3.1',
]
# the ensembl genomes link structure is like:
# fasta: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/fasta/arabidopsis_thaliana/dna/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa.gz
# cache: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/vep/arabidopsis_thaliana_vep_35_TAIR10.tar.gz
# so this dictionary is:
# {division: ['species.assembly', ...], ...}
species_ensembl_genomes = {
    'metazoa': [
        'Aedes_aegypti.AaegL3',
        'Anopheles_gambiae.AgamP4',
        'Schistosoma_mansoni.ASM23792v2',
        'Strongyloides_ratti.S_ratti_ED321_v5_0_4',
    ],
    'plants': [
        'Arabidopsis_thaliana.TAIR10',
        # in EVA we have:030312v2 'Hordeum_vulgare.ASM32608v1'
        'Oryza_sativa.IRGSP-1.0',
        # in EVA we have:sorbi1 'Sorghum_bicolor.Sorghum_bicolor_v2',
        # in EVA we have:sl240 'Solanum_lycopersicum.SL2.50',
        # in EVA we have:agpv3 'Zea_mays.AGPv4'
    ]
}


# fasta: ftp://ftp.ensembl.org/pub/release-89/fasta/bos_taurus/dna/Bos_taurus.UMD3.1.dna.toplevel.fa.gz
def build_fasta_name(version, species_assembly):
    return '{}.dna.toplevel.fa.gz'.format(species_assembly)

def build_fasta_path_ensembl(version, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return 'pub/release-{}/fasta/{}/dna/{}'.format(
        version,
        species.lower(),
        build_fasta_name(version, species_assembly)
    )


# cache: ftp://ftp.ensembl.org/pub/release-89/variation/VEP/bos_taurus_vep_89_UMD3.1.tar.gz
def build_cache_name(version, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return '{}_vep_{}_{}.tar.gz'.format(
        species.lower(),
        version,
        assembly
    )

def build_cache_path_ensembl(version, species_assembly):
    return 'pub/release-{}/variation/VEP/{}'.format(
        version,
        build_cache_name(version, species_assembly)
    )

def download_file(domain, path, file_name):
    if (args.test):
        try:
            size = ftp.size(path)
            print('{} bytes is the size of {}/{}'.format(size, domain, path))
        except error_perm:
            print('ERROR: {}/{} does not exist'.format(domain, path))
    else:
        print('downloading {}/{} into {}'.format(domain, path, file_name))
        try:
            ftp.retrbinary('RETR ' + path, open(file_name, 'wb').write)
        except error_perm:
            print('ERROR: {}/{} does not exist'.format(domain, path))


ftp_ensembl_org = 'ftp.ensembl.org'
ftp = FTP(ftp_ensembl_org)
ftp.login()
for species in species_ensembl:
    fasta_path = build_fasta_path_ensembl(args.ensembl_version, species)
    fasta_name = build_fasta_name(args.ensembl_version, species)
    download_file(ftp_ensembl_org, fasta_path, fasta_name)
    cache_path = build_cache_path_ensembl(args.ensembl_version, species)
    cache_name = build_cache_name(args.ensembl_version, species)
    download_file(ftp_ensembl_org, cache_path, cache_name)

ftp.quit()


# fasta: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/fasta/arabidopsis_thaliana/dna/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa.gz
def build_fasta_path_genomes(version, division, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return 'pub/release-{}/{}/fasta/{}/dna/{}'.format(
        version,
        division,
        species.lower(),
        build_fasta_name(version, species_assembly)
    )


# cache: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/vep/arabidopsis_thaliana_vep_35_TAIR10.tar.gz
def build_cache_path_genomes(version, division, species_assembly):
    return 'pub/release-{}/{}/vep/{}'.format(
        version,
        division,
        build_cache_name(version, species_assembly)
    )

ftp_ensemblgenomes_org = 'ftp.ensemblgenomes.org'
ftp = FTP(ftp_ensemblgenomes_org)
ftp.login()

for division in species_ensembl_genomes:
    for species in species_ensembl_genomes[division]:
        fasta_path = build_fasta_path_genomes(args.ensembl_genomes_version, division, species)
        fasta_name = build_fasta_name(args.ensembl_genomes_version, species)
        download_file(ftp_ensemblgenomes_org, fasta_path, fasta_name)
        cache_path = build_cache_path_genomes(args.ensembl_genomes_version, division, species)
        cache_name = build_cache_name(args.ensembl_genomes_version, species)
        download_file(ftp_ensemblgenomes_org, cache_path, cache_name)

ftp.quit()
