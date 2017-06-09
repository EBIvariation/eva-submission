#!/usr/bin/python

import argparse
import string
from ftplib import FTP, error_perm
import tarfile
import subprocess
import shutil
import os
import logging


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


# fasta: Bos_taurus.UMD3.1.dna.toplevel.fa.gz
def build_fasta_name(species_assembly):
    return '{}.dna.toplevel.fa.gz'.format(species_assembly)


# cache: bos_taurus_vep_89_UMD3.1.tar.gz
def build_cache_name(version, species_assembly):
    [species, assembly] = string.split(species_assembly, '.', 1)
    return '{}_vep_{}_{}.tar.gz'.format(
        species.lower(),
        version,
        assembly
    )


def check_size(ftp, domain, path):
    try:
        remote_size = ftp.size(path)
        print('{} bytes is the size of {}/{}'.format(remote_size, domain, path))
        return remote_size
    except error_perm as error:
        logging.error('{}/{} does not exist'.format(domain, path))
        raise error


def download_file(ftp, domain, path, file_name, remote_size):
    print('Downloading {}/{} into {}'.format(domain, path, file_name))
    try:
        ftp.retrbinary('RETR ' + path, open(file_name, 'wb').write)
    except error_perm as error:
        logging.error('{}/{} does not exist'.format(domain, path))
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


def decompress_fasta(version, species):
    print('Decompressing fasta for ' + species)
    fasta_name = build_fasta_name(species)
    subprocess.call(['gunzip', fasta_name])
    shutil.move(fasta_name[0:-3], build_dest_fasta_folder(version, species))


def decompress_cache(version, species):
    print('Decompressing cache for ' + species)
    compressed_cache = build_cache_name(version, species)
    with tarfile.open(compressed_cache) as cache_file:
        cache_file.extractall()
    os.remove(compressed_cache)


class EnsemblGenomesFastaFile:
    def __init__(self, version, division, species):
        self.version = version
        self.division = division
        self.species = species

    # fasta: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/fasta/arabidopsis_thaliana/dna/Arabidopsis_thaliana.TAIR10.dna.toplevel.fa.gz
    @property
    def path(self):
        [species, assembly] = string.split(self.species, '.', 1)
        return 'pub/release-{}/{}/fasta/{}/dna/{}'.format(
            self.version,
            self.division,
            species.lower(),
            self.name
        )

    @property
    def name(self):
        return build_fasta_name(self.species)

    def decompress(self):
        decompress_fasta(self.version, self.species)


class EnsemblGenomesCacheFile:
    def __init__(self, version, division, species):
        self.version = version
        self.division = division
        self.species = species

    # cache: ftp://ftp.ensemblgenomes.org/pub/release-35/plants/vep/arabidopsis_thaliana_vep_35_TAIR10.tar.gz
    @property
    def path(self):
        return 'pub/release-{}/{}/vep/{}'.format(
            self.version,
            self.division,
            self.name
        )

    @property
    def name(self):
        return build_cache_name(self.version, self.species)

    def decompress(self):
        decompress_cache(self.version, self.species)


class EnsemblGenomesDownloader:
    species_list = species_ensembl_genomes
    domain = 'ftp.ensemblgenomes.org'
    ftp = FTP(domain)

    def __init__(self, version, test):
        self.version = version
        self.test = test

    def download_caches_and_fastas(self):
        self.ftp.login()
        for division in self.species_list:
            for species in self.species_list[division]:
                try:
                    download(self, EnsemblGenomesCacheFile(self.version, division, species),
                             self.test)
                    download(self, EnsemblGenomesFastaFile(self.version, division, species),
                             self.test)
                except error_perm as e:
                    logging.error(e.message)
                    pass
        self.ftp.quit()


class EnsemblFastaFile:
    def __init__(self, version, species):
        self.version = version
        self.species = species

    # fasta: ftp://ftp.ensembl.org/pub/release-89/fasta/bos_taurus/dna/Bos_taurus.UMD3.1.dna.toplevel.fa.gz
    @property
    def path(self):
        [species, assembly] = string.split(self.species, '.', 1)
        return 'pub/release-{}/fasta/{}/dna/{}'.format(
            self.version,
            species.lower(),
            self.name
        )

    @property
    def name(self):
        return build_fasta_name(self.species)

    def decompress(self):
        decompress_fasta(self.version, self.species)


class EnsemblCacheFile:
    def __init__(self, version, species):
        self.version = version
        self.species = species

    # cache: ftp://ftp.ensembl.org/pub/release-89/variation/VEP/bos_taurus_vep_89_UMD3.1.tar.gz
    @property
    def path(self):
        return 'pub/release-{}/variation/VEP/{}'.format(
            self.version,
            self.name
        )

    @property
    def name(self):
        return build_cache_name(self.version, self.species)

    def decompress(self):
        decompress_cache(self.version, self.species)


class EnsemblDownloader:
    species_list = species_ensembl
    domain = 'ftp.ensembl.org'
    ftp = FTP(domain)

    def __init__(self, version, test):
        self.version = version
        self.test = test

    def download_caches_and_fastas(self):
        self.ftp.login()
        for species in self.species_list:
            try:
                download(self, EnsemblCacheFile(self.version, species), self.test)
                download(self, EnsemblFastaFile(self.version, species), self.test)
            except error_perm as e:
                logging.error(e.message)
                pass
        self.ftp.quit()


def download(downloader, file_resolver, test):
    path = file_resolver.path
    size = check_size(downloader.ftp, downloader.domain, path)
    if not test:
        download_file(downloader.ftp, downloader.domain, path, file_resolver.name, size)
        file_resolver.decompress()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='This script downloads and decompresses a list of '
                                                 'caches from Ensembl and Ensembl Genomes. The '
                                                 'parameters are the release versions of Ensembl '
                                                 'and Ensembl Genomes. Both are optional. It is '
                                                 'recommended to run first with an extra -t to '
                                                 'check that all files exist.')

    parser.add_argument('-e', '--ensembl-version', dest='ensembl_version',
                        help='release version of the Ensembl variation cache')
    parser.add_argument('-g', '--ensembl-genomes-version', dest='ensembl_genomes_version',
                        help='release version of the Ensembl Genomes cache')
    parser.add_argument('-t', '--test', action='store_true', dest='test',
                        help='check that all the files exist in the remote ftp')
    args = parser.parse_args()

    if args.ensembl_version is None and args.ensembl_genomes_version is None:
        parser.print_help()

    if args.ensembl_version is not None:
        EnsemblDownloader(args.ensembl_version, args.test).download_caches_and_fastas()

    if args.ensembl_genomes_version is not None:
        EnsemblGenomesDownloader(
            args.ensembl_genomes_version,
            args.test
        ).download_caches_and_fastas()
