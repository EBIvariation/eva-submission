#!/usr/bin/python

import argparse
import string
from ftplib import FTP, error_perm
import tarfile
import subprocess
import shutil
import os

parser = argparse.ArgumentParser(description='This script downloads and decompresses a list of '
                                             'caches from ensembl and ensembl genomes. The '
                                             'parameters are the release versions of ensembl and '
                                             'ensembl genomes. Both are optional. It is '
                                             'recommended to run first with an extra -t to check '
                                             'that all files exist.')
parser.add_argument('-e', '--ensembl-version', dest='ensembl_version',
                    help='release version of the ensembl variation cache')
parser.add_argument('-g', '--ensembl-genomes-version', dest='ensembl_genomes_version',
                    help='release version of the ensembl genomes cache')
parser.add_argument('-t', '--test', action='store_true', dest='test',
                    help='check that all the files exist in the remote ftp')
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
def build_fasta_name(species_assembly):
    return '{}.dna.toplevel.fa.gz'.format(species_assembly)


def build_fasta_path_ensembl(version, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return 'pub/release-{}/fasta/{}/dna/{}'.format(
        version,
        species.lower(),
        build_fasta_name(species_assembly)
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


def download_file(ftp, domain, path, file_name):
    remote_size = 0
    try:
        remote_size = ftp.size(path)
        print('{} bytes is the size of {}/{}'.format(remote_size, domain, path))
    except error_perm as error:
        print('ERROR: {}/{} does not exist'.format(domain, path))
        raise error

    if not args.test:
        print('Downloading {}/{} into {}'.format(domain, path, file_name))
        try:
            ftp.retrbinary('RETR ' + path, open(file_name, 'wb').write)
        except error_perm as error:
            print('ERROR: {}/{} does not exist'.format(domain, path))
            raise error

        local_size = os.path.getsize(file_name)
        if local_size != remote_size:
            message = 'The sizes of remote and downloaded file ({}) do not match: {} and {}'.format(
                file_name, remote_size, local_size)
            raise error_perm(message)


def build_dest_fasta_folder(version, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return '{}/{}_{}/'.format(
        species.lower(),
        version,
        assembly
    )


def decompress_cache(version, species):
    print('Decompressing cache for ' + species)
    compressed_cache = build_cache_name(version, species)
    with tarfile.open(compressed_cache) as cache_file:
        cache_file.extractall()
    os.remove(compressed_cache)


def decompress_fasta(version, species):
    print('Decompressing fasta for ' + species)
    fasta_name = build_fasta_name(species)
    subprocess.call(['gunzip', fasta_name])
    shutil.move(fasta_name[0:-3], build_dest_fasta_folder(version, species))


class EnsemblDownloader:
    species_list = species_ensembl
    version = args.ensembl_version
    domain = 'ftp.ensembl.org'

    def download(self):
        ftp = FTP(self.domain)
        ftp.login()
        for species in species_ensembl:
            try:
                self.download_fasta(ftp, species)
                self.download_cache(ftp, species)
                if not args.test:
                    decompress_cache(self.version, species)
                    decompress_fasta(self.version, species)
            except error_perm:
                pass
        ftp.quit()

    def download_cache(self, ftp, species):
        cache_path = build_cache_path_ensembl(self.version, species)
        cache_name = build_cache_name(self.version, species)
        download_file(ftp, self.domain, cache_path, cache_name)

    def download_fasta(self, ftp, species):
        fasta_path = build_fasta_path_ensembl(self.version, species)
        fasta_name = build_fasta_name(species)
        download_file(ftp, self.domain, fasta_path, fasta_name)


# fasta: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/fasta/arabidopsis_thaliana/dna/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa.gz
def build_fasta_path_genomes(version, division, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return 'pub/release-{}/{}/fasta/{}/dna/{}'.format(
        version,
        division,
        species.lower(),
        build_fasta_name(species_assembly)
    )


# cache: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/vep/arabidopsis_thaliana_vep_35_TAIR10.tar.gz
def build_cache_path_genomes(version, division, species_assembly):
    return 'pub/release-{}/{}/vep/{}'.format(
        version,
        division,
        build_cache_name(version, species_assembly)
    )


class EnsemblGenomesDownloader:
    species_list = species_ensembl_genomes
    version = args.ensembl_genomes_version
    domain = 'ftp.ensemblgenomes.org'

    def download(self):
        ftp = FTP(self.domain)
        ftp.login()
        for division in self.species_list:
            for species in self.species_list[division]:
                try:
                    self.download_fasta(ftp, division, species)
                    self.download_cache(ftp, division, species)
                    if not args.test:
                        decompress_cache(self.version, species)
                        decompress_fasta(self.version, species)
                except error_perm:
                    pass
        ftp.quit()

    def download_fasta(self, ftp, division, species):
        fasta_path = build_fasta_path_genomes(self.version, division, species)
        fasta_name = build_fasta_name(species)
        download_file(ftp, self.domain, fasta_path, fasta_name)

    def download_cache(self, ftp, division, species):
        cache_path = build_cache_path_genomes(self.version, division, species)
        cache_name = build_cache_name(self.version, species)
        download_file(ftp, self.domain, cache_path, cache_name)


if args.ensembl_version is None and args.ensembl_genomes_version is None:
    parser.print_help()

if args.ensembl_version is not None:
    ensembl = EnsemblDownloader()
    ensembl.download()

if args.ensembl_genomes_version is not None:
    genomes = EnsemblGenomesDownloader()
    genomes.download()
