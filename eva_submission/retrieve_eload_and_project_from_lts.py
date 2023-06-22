import fileinput
import logging
import os

import yaml
from ebi_eva_common_pyutils import command_utils
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg

logger = log_cfg.get_logger(__name__)


class ELOADRetrieval:

    def create_dir_if_not_exist(self, dir_path):
        if not os.path.exists(dir_path):
            os.makedirs(dir_path)

    def get_compressed_files_in_dirs(self, dir_path):
        # To uncompress - retrieve every compressed file from the directory (including subdirectories)
        # but exclude compressed vcf files (the ones ending with .vcf.gz)
        if os.path.exists(dir_path):
            file_list = []
            for root, directories, files in os.walk(dir_path):
                for file in files:
                    if file[-3:] == '.gz' and file[-7:] != '.vcf.gz':
                        file_list.append(os.path.join(root, file))
            return file_list
        else:
            return []

    def uncompress_files(self, files_to_uncompress):
        for file in files_to_uncompress:
            command = f"gzip -f -dv {file}"
            command_utils.run_command_with_output('Uncompress file', command)

    def get_project_from_eload_config(self, retrieved_dir, archive_name):
        config_file_path = os.path.join(retrieved_dir, archive_name, f'.{archive_name}_config.yml')

        if os.path.isfile(config_file_path):
            with open(config_file_path, 'r') as eload_config:
                eload_content = yaml.safe_load(eload_config)
                try:
                    project_acc = eload_content['brokering']['ena']['PROJECT']
                    return project_acc
                except:
                    logging.warning(f'No Project accession found in ELOAD config for ELOAD {archive_name}')
        else:
            logger.warning(f"No ELOAD config file found for eload: {archive_name}")

    def update_path_in_eload_config(self, retrieved_dir, archive_name):
        config_file_path = os.path.join(retrieved_dir, archive_name, f'.{archive_name}_config.yml')

        if os.path.isfile(config_file_path):
            with fileinput.input(config_file_path, inplace=True) as file:
                for line in file:
                    new_line = line.replace('/nfs/production3/', '/nfs/production/keane/')
                    print(new_line, end='')

    def retrieve_archive(self, archive_path, retrieval_output_path, files_dirs_to_retrieve=''):
        if os.path.exists(archive_path):
            command = f"tar -xf {archive_path} -C {retrieval_output_path} {files_dirs_to_retrieve}"
            command_utils.run_command_with_output('Retrieve files/dir from tar', command)
        else:
            logger.error(f'Archive path {archive_path} does not exist')

    def get_files_or_dirs_to_retrieve_from_archive(self, archive):
        files_dirs_to_retrieve = []
        if 'retrieve_dirs' in archive and archive['retrieve_dirs']:
            files_dirs_to_retrieve.extend(archive['retrieve_dirs'])
        if 'retrieve_files' in archive and archive['retrieve_files']:
            files_dirs_to_retrieve.extend(archive['retrieve_files'])

        return ' '.join(files_dirs_to_retrieve)

    def retrieve_eload(self, eload, retrieve_associated_project, update_path, eload_dirs_files, eload_lts_dir,
                       eload_output_dir):
        self.create_dir_if_not_exist(eload_output_dir)
        logging.info(f"Retrieving Eloads")

        # Retrieve eload
        eload_tar = f'{eload}.tar'
        eload_archive_path = os.path.join(eload_lts_dir, eload_tar)
        retrieve_dirs_files = " ".join(eload_dirs_files) if eload_dirs_files and len(eload_dirs_files) > 0 else ''
        self.retrieve_archive(eload_archive_path, eload_output_dir, retrieve_dirs_files)

        # Uncompress files
        files_to_uncompress = self.get_compressed_files_in_dirs(os.path.join(eload_output_dir, eload))
        self.uncompress_files(files_to_uncompress)

        # Update noah paths to codon in eload config
        if update_path:
            self.update_path_in_eload_config(eload_output_dir, eload)

        # Retrieve associated project if specified
        if retrieve_associated_project:
            return self.get_project_from_eload_config(eload_output_dir, eload)

    def retrieve_project(self, project, project_dirs_files, project_lts_dir, project_output_dir):
        logging.info(f"Retrieving Project")
        self.create_dir_if_not_exist(project_output_dir)

        project_tar = f'{project}.tar'
        project_archive_path = os.path.join(project_lts_dir, project_tar)

        retrieve_dirs_files = " ".join(project_dirs_files) if project_dirs_files and len(project_dirs_files) > 0 else ''
        self.retrieve_archive(project_archive_path, project_output_dir, retrieve_dirs_files)

        # Uncompress files
        files_to_uncompress = self.get_compressed_files_in_dirs(os.path.join(project_output_dir, project))
        self.uncompress_files(files_to_uncompress)

    def retrieve_eloads_and_projects(self, eload, retrieve_associated_project, update_path, eload_dirs_files, project,
                                     project_dirs_files, eload_lts_dir, project_lts_dir, eload_retrieval_dir,
                                     project_retrieval_dir):
        eload_lts_dir = eload_lts_dir or cfg['eloads_lts_dir']
        project_lts_dir = project_lts_dir or cfg['projects_lts_dir']
        eload_retrieval_dir = eload_retrieval_dir or cfg['eloads_dir']
        project_retrieval_dir = project_retrieval_dir or cfg['projects_dir']

        if eload:
            project = self.retrieve_eload(f'ELOAD_{eload}', retrieve_associated_project, update_path, eload_dirs_files,
                                eload_lts_dir, eload_retrieval_dir)
        if project:
            self.retrieve_project(project, project_dirs_files, project_lts_dir, project_retrieval_dir)
