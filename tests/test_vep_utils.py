import os
import shutil
from unittest import TestCase
from unittest.mock import Mock

from ebi_eva_common_pyutils.config import cfg

from eva_submission.submission_config import load_config
from eva_submission.vep_utils import recursive_nlst, get_vep_and_vep_cache_version_from_ensembl


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
        self.assertEqual(vep_version, 104)
        self.assertEqual(cache_version, 51)
        assert os.path.exists(os.path.join(cfg['vep_cache_path'], 'thelohanellus_kitauei'))
