import logging
import os
import shutil
from unittest import TestCase
from unittest.mock import Mock, patch

import pytest
from ebi_eva_common_pyutils.config import cfg

from eva_submission.submission_config import load_config
from eva_submission.vep_utils import recursive_nlst, get_vep_and_vep_cache_version_from_ensembl, \
    get_vep_and_vep_cache_version, download_and_extract_vep_cache, get_ftp_connection, get_species_and_assembly


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
        vep_version, cache_version = get_vep_and_vep_cache_version_from_ensembl('GCA_000827895.1')
        self.assertEqual(vep_version, 110)
        self.assertEqual(cache_version, 57)
        assert os.path.exists(os.path.join(cfg['vep_cache_path'], 'thelohanellus_kitauei'))
        assert os.listdir(os.path.join(cfg['vep_cache_path'], 'thelohanellus_kitauei')) == ['57_ASM82789v1']

    def test_get_vep_versions_from_ensembl_not_found(self):
        vep_version, cache_version = get_vep_and_vep_cache_version_from_ensembl('GCA_015220235.1')
        self.assertEqual(vep_version, None)
        self.assertEqual(cache_version, None)

    @pytest.mark.skip(reason='Too slow to run as is, which makes deployment difficult')
    def test_get_vep_versions_from_ensembl_older_version(self):
        # Older version of assembly using NCBI assembly code isn't found successfully
        vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_000002765.1')
        self.assertEqual(vep_version, None)
        self.assertEqual(cache_version, None)
        # If we magically knew the Ensembl assembly code was EPr1 we could find it!
        vep_version, cache_version, vep_species = get_vep_and_vep_cache_version_from_ensembl('GCA_000002765.1', 'EPr1')
        self.assertEqual(vep_version, 44 + 53)
        self.assertEqual(cache_version, 44)
        self.assertEqual(vep_species, 'plasmodium_falciparum')

    def test_get_vep_versions(self):
        with patch('eva_submission.vep_utils.get_vep_and_vep_cache_version_from_db') as m_get_db, \
                patch('eva_submission.vep_utils.get_vep_and_vep_cache_version_from_ensembl') as m_get_ensembl, \
                patch('eva_submission.vep_utils.get_species_and_assembly') as m_get_species:
            # If db has versions, use those
            m_get_db.return_value = (104, 104)
            m_get_species.return_value = ('homo_sapiens', None, None)
            m_get_ensembl.return_value = (97, 97)
            vep_version, vep_cache_version = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, 104)
            self.assertEqual(vep_cache_version, 104)

            # If db has no versions but Ensembl does, use those
            m_get_db.return_value = (None, None)
            m_get_ensembl.return_value = (97, 97)
            vep_version, vep_cache_version = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, 97)
            self.assertEqual(vep_cache_version, 97)

            # If neither has any versions, return none
            m_get_db.return_value = (None, None)
            m_get_ensembl.return_value = (None, None)
            vep_version, vep_cache_version = get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')
            self.assertEqual(vep_version, None)
            self.assertEqual(vep_cache_version, None)

            # If a VEP version is not installed, raise an error
            m_get_db.return_value = (1, 1)
            m_get_ensembl.return_value = (None, None)
            m_get_species.return_value = ('homo_sapiens', None, None)
            with self.assertRaises(ValueError):
                get_vep_and_vep_cache_version('fake_mongo', 'fake_db', 'fake_assembly')

    def test_download_and_extract_vep_cache(self):
        with patch('eva_submission.vep_utils.retrieve_species_scientific_name_from_tax_id_ncbi') as m_get_scf_name:
            m_get_scf_name.return_value = 'whatever_species_name'
            with get_ftp_connection('ftp.ensembl.org') as ftp_conn:
                download_and_extract_vep_cache(
                    ftp_conn,
                    '/pub/release-105/variation/indexed_vep_cache/papio_anubis_refseq_vep_105_Panubis1.0.tar.gz', 1001
                )
                assert os.path.exists(os.path.join(cfg['vep_cache_path'], 'whatever_species_name', '105_Panubis1.0'))

    def test_get_species_and_assembly(self):
        assemblies2results = {
            'GCA_000001405.1': ('homo_sapiens', 'GRCh37', False, '9606'),
            'GCA_000001405.14': ('homo_sapiens', 'GRCh37.p13', False, '9606'),
            'GCA_000001405.20': ('homo_sapiens', 'GRCh38.p5', False, '9606'),
            'GCA_000001405.29': ('homo_sapiens', 'GRCh38.p14', True, '9606'),
            'GCA_000001635.2': ('mus_musculus', 'GRCm38', False, '10090'),
            'GCA_000002285.2': ('canis_lupus_familiaris', 'CanFam3.1', False, '9615'),
            'GCA_000002315.5': ('gallus_gallus', 'GRCg6a', True, '9031'),
            'GCA_000003025.6': ('sus_scrofa', 'Sscrofa11.1', True, '9823'),
            'GCA_000181335.4': ('felis_catus', 'Felis_catus_9.0', True, '9685'),
            'GCA_000473445.2': ('anopheles_farauti', 'Anop_fara_FAR1_V2', True, '69004'),
            'GCA_001704415.1': ('capra_hircus', 'ARS1', True, '9925'),
            'GCA_002263795.2': ('bos_taurus', 'ARS-UCD1.2', True, '9913'),
            'GCA_002742125.1': ('ovis_aries_rambouillet', 'Oar_rambouillet_v1.0', True, '9940'),
            'GCA_002863925.1': ('equus_caballus', 'EquCab3.0', True, '9796')
        }
        for assembly in assemblies2results:
            res = get_species_and_assembly(assembly)
            assert res == assemblies2results.get(assembly)
