import fileinput
import logging
import os
from argparse import ArgumentParser

import yaml
from ebi_eva_common_pyutils import command_utils

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def get_compressed_files_in_dirs(dir_path):
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


def uncompress_files(files_to_uncompress):
    for file in files_to_uncompress:
        command = f"gzip -f -dv {file}"
        command_utils.run_command_with_output('Uncompress file', command)


def get_project_from_eload_config(retrieved_dir, archive_name):
    archive_name = archive_name.rstrip('.tar')
    config_file_path = os.path.join(retrieved_dir, archive_name, f'.{archive_name}_config.yml')

    if os.path.isfile(config_file_path):
        with open(config_file_path, 'r') as eload_config:
            eload_content = yaml.safe_load(eload_config)
            try:
                project_acc = eload_content['brokering']['ena']['PROJECT']
                return project_acc
            except:
                logging.warning(f'Project accession could not be retrieved for ELOAD {archive_name}')


def update_path_in_eload_config(retrieved_dir, archive_name):
    archive_name = archive_name.rstrip('.tar')
    config_file_path = os.path.join(retrieved_dir, archive_name, f'.{archive_name}_config.yml')

    if os.path.isfile(config_file_path):
        with fileinput.input(config_file_path, inplace=True) as file:
            for line in file:
                new_line = line.replace('/nfs/production3', '/nfs/production/keane')
                print(new_line, end='')


def retrieve_archive(archive_path, retrieval_output_path, files_dirs_to_retrieve=''):
    command = f"tar -xf {archive_path} -C {retrieval_output_path} {files_dirs_to_retrieve}"
    command_utils.run_command_with_output('Retrieve files/dir from tar', command)


def get_files_or_dirs_to_retrieve_from_archive(archive):
    files_dirs_to_retrieve = []
    if 'retrieve_dirs' in archive and archive['retrieve_dirs']:
        files_dirs_to_retrieve.extend(archive['retrieve_dirs'])
    if 'retrieve_files' in archive and archive['retrieve_files']:
        files_dirs_to_retrieve.extend(archive['retrieve_files'])

    return ' '.join(files_dirs_to_retrieve)


def retrieve_eloads(eloads_archive_info, eloads_archive_dir, projects_archive_dir):
    eloads_output_path = eloads_archive_info['eloads_output_path']
    projects_output_path = eloads_archive_info['projects_output_path']

    if not os.path.exists(eloads_output_path):
        os.makedirs(eloads_output_path)

    logging.info(f"Retrieving Eloads")
    for eload_archive_info in eloads_archive_info['eload_archives']:
        ## Retrieve eload
        eload_name = eload_archive_info['name']
        eload_archive_path = os.path.join(eloads_archive_dir, eload_name)
        files_dirs_to_retrieve = get_files_or_dirs_to_retrieve_from_archive(eload_archive_info)
        retrieve_archive(eload_archive_path, eloads_output_path, files_dirs_to_retrieve)

        # Uncompress files
        files_to_uncompress = get_compressed_files_in_dirs(
            os.path.join(eloads_output_path, eload_name.rstrip('.tar')))
        uncompress_files(files_to_uncompress)

        # Retrieve associated project if specified
        if eload_archive_info['retrieve_associated_project']:
            project_acc = get_project_from_eload_config(eloads_output_path, eload_name)
            if project_acc:
                retrieve_archive(os.path.join(projects_archive_dir, f'{project_acc}.tar'), projects_output_path)
                project_files_to_uncompress = get_compressed_files_in_dirs(os.path.join(projects_output_path, project_acc))
                uncompress_files(project_files_to_uncompress)

        # Update noah paths to codon in eload config
        if eload_archive_info['update_noah_paths']:
            update_path_in_eload_config(eloads_output_path, eload_name)


def retrieve_projects(project_archive_info, projects_archive_dir):
    projects_output_path = project_archive_info['projects_output_path']

    if not os.path.exists(projects_output_path):
        os.makedirs(projects_output_path)

    logging.info(f"Retrieving Projects")
    for project_archive_info in project_archive_info['project_archives']:
        # Retrieve Project
        project_name = project_archive_info['name']
        project_archive_path = os.path.join(projects_archive_dir, project_name)
        files_dirs_to_retrieve = get_files_or_dirs_to_retrieve_from_archive(project_archive_path)
        retrieve_archive(project_archive_path, projects_output_path, files_dirs_to_retrieve)

        # Uncompress files
        files_to_uncompress = get_compressed_files_in_dirs(
            os.path.join(projects_output_path, project_name.rstrip('.tar')))
        uncompress_files(files_to_uncompress)


def retrieve_eloads_and_projects(config_file):
    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)

            for archive_type in config['retrieve']:
                if archive_type == 'eloads':
                    retrieve_eloads(config['retrieve']['eloads'], config['retrieve']['eloads_archive_dir'],
                                    config['retrieve']['projects_archive_dir'])
                elif archive_type == 'projects':
                    retrieve_projects(config['retrieve']['projects'], config['retrieve']['projects_archive_dir'])


def main():
    argparse = ArgumentParser(description='Accession and ingest submission data into EVA')
    argparse.add_argument("--config_file", required=True, type=str, help='Path to the config yml file')

    args = argparse.parse_args()

    retrieve_eloads_and_projects(args.config_file)


if __name__ == "__main__":
    main()
