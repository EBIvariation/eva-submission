import logging
import os
import shutil
from unittest import TestCase
from unittest.mock import Mock, patch

from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config

from eva_submission.submission_config import load_config
from eva_submission.vep_utils import recursive_nlst, get_vep_and_vep_cache_version_from_ensembl, \
    get_vep_and_vep_cache_version, download_and_extract_vep_cache, get_ftp_connection


class TestVepUtils(TestCase):
    top_dir = os.path.dirname(os.path.dirname(__file__))
    resources_folder = os.path.join(os.path.dirname(__file__), 'resources')

    def setUp(self):
        config_file = os.path.join(self.resources_folder, 'submission_config.yml')
        load_config(config_file)
        # Need to set the directory so that the relative path set in the config file works from the top directory
        os.chdir(self.top_dir)

        # Set up vep cache directory and vep
        os.makedirs(cfg['vep_cache_path'], exist_ok=True)
        os.makedirs(os.path.join(cfg['vep_path'], 'ensembl-vep-release-104/vep'), exist_ok=True)
        os.makedirs(os.path.join(cfg['vep_path'], 'ensembl-vep-release-97/vep'), exist_ok=True)

    def tearDown(self):
        shutil.rmtree(cfg['vep_cache_path'])
        shutil.rmtree(cfg['vep_path'])

    def test_recursive_nlst(self):
        # Mock dir() method in ftplib to reflect the following file structure:
        #   root/
        #     - 1_collection/
        #         - 1_collection.tar.gz
        #         - something.txt
        #     - 2_collection/
        #         - 2_collection.tar.gz
        #         - something.txt
        #     - root.tar.gz
        def fake_dir(path, callback):
            filename = path.split('/')[-1] + '.tar.gz'
            root_output = f'''drwxrwxr-x    2 ftp      ftp        102400 Apr 13 13:47 1_collection
drwxrwxr-x    2 ftp      ftp        102400 Apr 13 13:59 2_collection
-rw-rw-r--    1 ftp      ftp       4410832 Apr 13 13:59 {filename}'''
            subdir_output = f'''-rw-rw-r--    1 ftp      ftp       2206830 Apr 13 13:52 {filename}
-rw-rw-r--    1 ftp      ftp       2206830 Apr 13 13:52 something.txt'''
            if path.endswith('collection'):
                callback(subdir_output)
            else:
                callback(root_output)

        ftp = Mock()
        ftp.dir.side_effect = fake_dir

        all_files = sorted(recursive_nlst(ftp, 'root', '*.tar.gz'))
        self.assertEqual(
            all_files,
            ['root/1_collection/1_collection.tar.gz', 'root/2_collection/2_collection.tar.gz', 'root/root.tar.gz']
        )

    def test_get_vep_versions_from_ensembl(self):
        vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_000827895.1')
        self.assertEqual(vep_version, 105)
        self.assertEqual(cache_version, 52)
        self.assertEqual(vep_species, 'thelohanellus_kitauei')
        assert os.path.exists(os.path.join(cfg['vep_cache_path'], 'thelohanellus_kitauei'))
        assert os.listdir(os.path.join(cfg['vep_cache_path'], 'thelohanellus_kitauei')) == ['52_ASM82789v1']

    def test_get_vep_versions_from_ensembl_not_found(self):
        vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_015220235.1')
        self.assertEqual(vep_version, None)
        self.assertEqual(cache_version, None)
        self.assertEqual(vep_species, None)

    # DISABLED because too slow and make deployment difficult.
    # def test_get_vep_versions_from_ensembl_older_version(self):
    #     # Older version of assembly using NCBI assembly code isn't found successfully
    #     # TODO this takes about 20 minutes to finish when I test locally
    #     vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_000002765.1')
    #     self.assertEqual(vep_version, None)
    #     self.assertEqual(cache_version, None)
    #     # If we magically knew the Ensembl assembly code was EPr1 we could find it!
    #     vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_000002765.1', 'EPr1')
    #     self.assertEqual(vep_version, 44 + 53)
    #     self.assertEqual(cache_version, 44)
    #     self.assertEqual(vep_species, 'plasmodium_falciparum')

    def test_get_vep_versions(self):
        with patch('eva_submission.vep_utils.get_vep_and_vep_cache_version_from_db') as m_get_db, \
                patch('eva_submission.vep_utils.get_vep_and_vep_cache_version_from_ensembl') as m_get_ensembl, \
                patch('eva_submission.vep_utils.get_species_and_assembly') as m_get_species:
            # If db has versions, use those
            m_get_db.return_value = (104, 104)
            m_get_species.return_value = ('homo_sapiens', None, None)
            m_get_ensembl.return_value = (97, 97, 'homo_sapiens')
            vep_version, vep_cache_version, vep_species = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, 104)
            self.assertEqual(vep_cache_version, 104)
            self.assertEqual(vep_species, 'homo_sapiens')

            # If db has no versions but Ensembl does, use those
            m_get_db.return_value = (None, None)
            m_get_ensembl.return_value = (97, 97, 'homo_sapiens')
            vep_version, vep_cache_version, vep_species = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, 97)
            self.assertEqual(vep_cache_version, 97)
            self.assertEqual(vep_species, 'homo_sapiens')

            # If neither has any versions, return none
            m_get_db.return_value = (None, None)
            m_get_ensembl.return_value = (None, None, None)
            vep_version, vep_cache_version, vep_species = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, None)
            self.assertEqual(vep_cache_version, None)

            # If a VEP version is not installed, raise an error
            m_get_db.return_value = (1, 1)
            m_get_ensembl.return_value = (None, None, 'homo_sapiens')
            m_get_species.return_value = ('homo_sapiens', None, None)
            with self.assertRaises(ValueError):
                get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')

    def test_download_and_extract_vep_cache(self):
        species_name = 'whatever_species_name'
        download_and_extract_vep_cache(
            get_ftp_connection('ftp.ensembl.org'), species_name,
            '/pub/release-105/variation/indexed_vep_cache/papio_anubis_refseq_vep_105_Panubis1.0.tar.gz'
        )
        assert os.path.exists(os.path.join(cfg['vep_cache_path'], species_name, '105_Panubis1.0'))
