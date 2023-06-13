import fileinput
import logging
import os
from argparse import ArgumentParser

import yaml
from ebi_eva_common_pyutils import command_utils

logging.basicConfig(format='%(message)s', level=logging.DEBUG)


def get_compressed_files_in_dirs(dir_path, include_sub_dirs=False):
    if os.path.exists(dir_path):
        if include_sub_dirs:
            files_to_uncompress = []
            for dirpath, _, filenames in os.walk(dir_path):
                for f in filenames:
                    if f[-3:] == '.gz':
                        files_to_uncompress.append(os.path.join(dirpath, f))
            return files_to_uncompress
        else:
            return [os.path.join(dir_path, f) for f in os.listdir(dir_path) if
                    os.path.isfile(os.path.join(dir_path, f)) and f[-3:] == '.gz']
    else:
        return []


def get_eloads_files_to_uncompress(retrieved_dir):
    # 10_submitted
    # 14_merge
    # 18_brokering
    # 20_scratch
    # 13_validation
    umcompress_files_from_dirs = [retrieved_dir]
    umcompress_files_from_dirs.append(get_compressed_files_in_dirs(os.path.join(retrieved_dir, '10_submitted', 'metadata_file'),
                                     include_sub_dirs=True))
    return get_compressed_files_in_dirs(umcompress_files_from_dirs)


def get_projects_files_to_uncompress(retrieved_dir):
    # 40_transformed
    # 53_clustering
    # 70_external_submissions
    # 80_deprecated
    # 30_eva_valid
    # 52_accessions
    # 60_eva_public
    # 50_stats
    # 00_logs
    # 51_annotation

    file_to_uncompress = []
    file_to_uncompress.extend(get_compressed_files_in_dirs(retrieved_dir))
    file_to_uncompress.extend(
        get_compressed_files_in_dirs(os.path.join(retrieved_dir, '00_logs'), include_sub_dirs=True))

    return file_to_uncompress


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


def retrieve_archive(archive_path, files_dirs_to_retrieve, retrieval_output_path):
    command = f"tar -xf {archive_path} -C {retrieval_output_path} {files_dirs_to_retrieve}"
    command_utils.run_command_with_output('Retrieve files/dir from tar', command)


def get_files_or_dirs_to_retrieve_from_archive(archive):
    files_dirs_to_retrieve = []
    if 'retrieve_dirs' in archive and archive['retrieve_dirs']:
        files_dirs_to_retrieve.extend(archive['retrieve_dirs'])
    if 'retrieve_files' in archive and archive['retrieve_files']:
        files_dirs_to_retrieve.extend(archive['retrieve_files'])

    return ' '.join(files_dirs_to_retrieve)


def retrieve_eloads(archive_info):
    eloads_dir_path = archive_info['eloads_dir_path']
    retrieval_output_path = archive_info['retrieval_output_path']
    projects_dir_path = archive_info['projects_dir_path']

    if not os.path.exists(retrieval_output_path):
        os.makedirs(retrieval_output_path)

    logging.info(f"Retrieving Eloads")
    for eload_archive_info in archive_info['eloads']:
        ## Retrieve eload
        eload_name = eload_archive_info['name']
        eload_archive_path = os.path.join(eloads_dir_path, eload_name)
        files_dirs_to_retrieve = get_files_or_dirs_to_retrieve_from_archive(eload_archive_info)
        retrieve_archive(eload_archive_path, files_dirs_to_retrieve, retrieval_output_path)

        # Uncompress files
        files_list_to_uncompress = get_eloads_files_to_uncompress(
            os.path.join(retrieval_output_path, eload_name.rstrip('.tar')))
        uncompress_files(files_list_to_uncompress)

        # Retrieve associated project if specified
        if eload_archive_info['retrieve_associated_project']:
            project_acc = get_project_from_eload_config(retrieval_output_path, eload_name)
            if project_acc:
                # Project retrieval
                pass

        # Update noah paths to codon in eload config
        if eload_archive_info['update_noah_paths']:
            update_path_in_eload_config(retrieval_output_path, eload_name)


def retrieve_projects(archive_info):
    projects_dir_path = archive_info['projects_dir_path']
    retrieval_output_path = archive_info['retrieval_output_path']

    if not os.path.exists(retrieval_output_path):
        os.makedirs(retrieval_output_path)

    logging.info(f"Retrieving Projects")
    for project_archive_info in archive_info['projects']:
        # Retrieve Project
        project_name = project_archive_info['name']
        project_archive_path = os.path.join(projects_dir_path, project_name)
        files_dirs_to_retrieve = get_files_or_dirs_to_retrieve_from_archive(project_archive_path)
        retrieve_archive(project_archive_path, files_dirs_to_retrieve, retrieval_output_path)

        # Uncompress files
        files_list = get_projects_files_to_uncompress(os.path.join(retrieval_output_path, project_name.rstrip('.tar')))
        uncompress_files(files_list)


def retrieve_eloads_and_projects(config_file):
    if os.path.isfile(config_file):
        with open(config_file, 'r') as f:
            yaml_content = yaml.safe_load(f)

            for archive_retrieval_info in yaml_content['retrieve']:
                archive_type = archive_retrieval_info['archive_type']
                if archive_type == 'eloads':
                    retrieve_eloads(archive_retrieval_info)
                elif archive_type == 'projects':
                    retrieve_projects(archive_retrieval_info)


def main():
    argparse = ArgumentParser(description='Accession and ingest submission data into EVA')
    argparse.add_argument("--config_file", required=True, type=str, help='Path to the config yml file')

    args = argparse.parse_args()

    retrieve_eloads_and_projects(args.config_file)


if __name__ == "__main__":
    main()
