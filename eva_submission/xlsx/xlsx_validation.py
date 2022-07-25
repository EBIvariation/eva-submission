import os

import yaml
from cerberus import Validator
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_ensembl
from ebi_eva_common_pyutils.variation.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi
from requests import HTTPError

from eva_submission import ETC_DIR
from eva_submission.eload_utils import cast_list
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader


class EvaXlsxValidator(AppLogger):

    def __init__(self, metadata_file):
        self.metadata_file = metadata_file
        self.reader = EvaXlsxReader(metadata_file)
        self.metadata = {}
        for worksheet in self.reader.reader.valid_worksheets():
            self.metadata[worksheet] = self.reader._get_all_rows(worksheet)

        self.error_list = []

    def validate(self):
        self.cerberus_validation()
        self.complex_validation()
        self.semantic_validation()

    def cerberus_validation(self):
        """
        Leverage cerberus validation to check the format of the metadata.
        This function adds error statements to the errors attribute
        """
        config_file = os.path.join(ETC_DIR, 'eva_project_validation.yaml')
        with open(config_file) as open_file:
            validation_schema = yaml.safe_load(open_file)
        validator = Validator(validation_schema)
        validator.allow_unknown = True
        validator.validate(self.metadata)
        for sheet in validator.errors:
            for error1 in validator.errors[sheet]:
                for data_pos in error1:
                    # data_pos is 0 based position in the data that was provided to cerberus
                    # Convert this position to the excel row number
                    row_num = data_pos + self.reader.reader.base_row_offset(sheet) + 1
                    for error2 in error1[data_pos]:
                        for field_name in error2:
                            for error3 in error2[field_name]:
                                self.error_list.append(
                                    f'In Sheet {sheet}, Row {row_num}, field {field_name}: {error3}'
                                )

    def complex_validation(self):
        """
        More complex validation steps that cannot be expressed in Cerberus
        This function adds error statements to the errors attribute
        """
        analysis_aliases = [analysis_row['Analysis Alias'] for analysis_row in self.metadata['Analysis']]
        self.same_set(
            analysis_aliases,
            [analysis_alias for sample_row in self.metadata['Sample'] for analysis_alias in sample_row['Analysis Alias'].split(',')],
            'Analysis Alias', 'Samples'
        )
        self.same_set(analysis_aliases, [file_row['Analysis Alias'] for file_row in self.metadata['Files']], 'Analysis Alias', 'Files')

        project_titles = [analysis_row['Project Title'] for analysis_row in self.metadata['Project']]
        self.same_set(project_titles, [analysis_row['Project Title'] for analysis_row in self.metadata['Analysis']], 'Project Title', 'Analysis')

        for row in self.metadata['Sample']:
            self.group_of_fields_required(
                'Sample', row,
                ['Analysis Alias', 'Sample Accession', 'Sample ID'],
                ['Analysis Alias', 'Sample Name', 'Title', 'Tax Id', 'Scientific Name']
            )

    def semantic_validation(self):
        """
        Validation of the data that involve checking its meaning
        This function adds error statements to the errors attribute
        """
        # Check if the references can be retrieved
        references = set([row['Reference'] for row in self.metadata['Analysis'] if row['Reference']])
        for reference in references:
            accessions = retrieve_genbank_assembly_accessions_from_ncbi(reference)
            if len(accessions) == 0:
                self.error_list.append(f'In Analysis, Reference {reference} did not resolve to any accession')
            elif len(accessions) > 1:
                self.error_list.append(f'In Analysis, Reference {reference} resolve to more than one accession: {accessions}')

        # Check taxonomy scientific name pair
        taxid_and_species_list = set([(row['Tax Id'], row['Scientific Name']) for row in self.metadata['Sample'] if row['Tax Id']])
        for taxid, species in taxid_and_species_list:
            try:
                scientific_name = get_scientific_name_from_ensembl(int(taxid))
                if species != scientific_name:
                    self.error_list.append(
                        f'In Samples, Taxonomy {taxid} and scientific name {species} are inconsistent')
            except ValueError as e:
                self.error(str(e))
                self.error_list.append(str(e))
            except HTTPError as e:
                self.error(str(e))
                self.error_list.append(str(e))

    def group_of_fields_required(self, sheet_name, row, *args):
        if not any(
            [all(row.get(key) for key in group) for group in args]
        ):
            self.error_list.append(
                'In %s, row %s, one of this group of fields must be filled: %s -- %s' % (
                    sheet_name, row.get('row_num'),
                    ' or '.join([', '.join(group) for group in args]),
                    ' -- '.join((', '.join(('%s:%s' % (key, row[key]) for key in group)) for group in args)),
                )
            )

    def same_set(self, list1, list2, list1_desc, list2_desc):
        if not set(list1) == set(list2):
            list1_list2 = sorted(cast_list(set(list1).difference(list2)))
            list2_list1 = sorted(cast_list(set(list2).difference(list1)))
            errors = []
            if list1_list2:
                errors.append('%s present in %s not in %s' % (','.join(list1_list2), list1_desc, list2_desc))
            if list2_list1:
                errors.append('%s present in %s not in %s' % (','.join(list2_list1), list2_desc, list1_desc))
            self.error_list.append('Check %s vs %s: %s' % (list1_desc, list2_desc, ' -- '.join(errors)))
