import datetime
import os
from collections import Counter

import yaml
from cerberus import Validator
from ebi_eva_common_pyutils.assembly_utils import retrieve_genbank_assembly_accessions_from_ncbi
from ebi_eva_common_pyutils.biosamples_communicators import WebinHALCommunicator
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import AppLogger
from ebi_eva_common_pyutils.reference import NCBIAssembly
from ebi_eva_common_pyutils.taxonomy.taxonomy import get_scientific_name_from_taxonomy
from requests import HTTPError

from eva_submission import ETC_DIR
from eva_submission.eload_utils import cast_list, check_existing_project_in_ena, check_project_format
from eva_submission.xlsx.xlsx_parser_eva import EvaXlsxReader, EvaXlsxWriter

# Values coming from https://www.ebi.ac.uk/ena/browser/view/ERC000011
not_provided_check_list = ['not provided', 'not collected', 'restricted access', 'missing: control sample',
                           'missing: sample group', 'missing: synthetic construct', 'missing: lab stock',
                           'missing: third party data', 'missing: data agreement established pre-2023',
                           'missing: endangered species', 'missing: human-identifiable']

class EvaXlsxValidator(AppLogger):

    def __init__(self, metadata_file):
        self.metadata_file = metadata_file
        self.reader = EvaXlsxReader(metadata_file)
        self.metadata = {}
        for worksheet in self.reader.reader.valid_worksheets():
            self.metadata[worksheet] = self.reader._get_all_rows(worksheet)

        self.error_list = []
        self.communicator = WebinHALCommunicator(
            cfg.query('biosamples', 'webin_url'), cfg.query('biosamples', 'bsd_url'),
            cfg.query('biosamples', 'webin_username'), cfg.query('biosamples', 'webin_password')
        )


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
        # Ensure analysis alias are unique in the Analysis sheet
        if len(set(analysis_aliases)) != len(analysis_aliases):
            counter = Counter(analysis_aliases)
            for analysis_alias in counter:
                if counter[analysis_alias] > 1:
                    self.error_list.append(f'Analysis alias {analysis_alias} is present {counter.get(analysis_alias)} times in the Analysis Sheet')
        self.same_set(
            analysis_aliases,
            [analysis_alias.strip() for sample_row in self.metadata['Sample'] for analysis_alias in sample_row['Analysis Alias'].split(',')],
            'Analysis Alias', 'Samples'
        )
        self.same_set(analysis_aliases, [file_row['Analysis Alias'] for file_row in self.metadata['Files']], 'Analysis Alias', 'Files')

        project_titles = [project_row['Project Title'] for project_row in self.metadata['Project']]
        self.same_set(project_titles, [analysis_row['Project Title'] for analysis_row in self.metadata['Analysis']], 'Project Title', 'Analysis')

        for row in self.metadata['Sample']:
            self.group_of_fields_required(
                'Sample', row,
                ['Analysis Alias', 'Sample Accession', 'Sample ID'],
                ['Analysis Alias', 'Sample Name', 'Title', 'Tax Id', 'Scientific Name', 'collection_date',
                 'geographic location (country and/or sea)']
            )
            # We check the collection_date only if it is a novel samples, or we're updating the existing sample
            if not row.get('Sample Accession') or row.get('collection_date'):
                self.check_date(row, 'collection_date', required=True)

    def semantic_validation(self):
        """
        Validation of the data that involve checking its meaning
        This function adds error statements to the errors attribute
        """
        self.check_reference_genome()
        self.check_taxonomy_scientific_name()
        self.check_biosamples_accessions()
        self.check_project_accessions()

    def check_reference_genome(self):
        """Check if the references can be retrieved"""
        references = set([row['Reference'] for row in self.metadata['Analysis'] if row['Reference']])
        for reference in references:
            accessions = retrieve_genbank_assembly_accessions_from_ncbi(reference, api_key=cfg.get('eutils_api_key'))
            # if the searched term is an actual genome GCA accession:
            if NCBIAssembly.is_assembly_accession_format(reference) and reference in accessions:
                accessions = {reference}
            if len(accessions) == 0:
                self.error_list.append(f'In Analysis, Reference {reference} did not resolve to any accession')
            elif len(accessions) > 1:
                self.error_list.append(f'In Analysis, Reference {reference} resolve to more than one accession: {accessions}')

    def check_taxonomy_scientific_name(self):
        """Check taxonomy scientific name pair"""
        correct_taxid_sc_name = {}
        taxid_and_species_list = set([(row['Tax Id'], row['Scientific Name']) for row in self.metadata['Sample'] if row['Tax Id']])
        for taxid, species in taxid_and_species_list:
            try:
                scientific_name = get_scientific_name_from_taxonomy(int(taxid))
                if species != scientific_name:
                    if species.lower() == scientific_name.lower():
                        correct_taxid_sc_name[taxid] = scientific_name
                    else:
                        self.error_list.append(
                            f'In Samples, Taxonomy {taxid} ({scientific_name}) and scientific name {species} are inconsistent')
            except ValueError as e:
                self.error(str(e))
                self.error_list.append(str(e))
            except HTTPError as e:
                self.error(str(e))
                self.error_list.append(str(e))
        if correct_taxid_sc_name:
            self.warning(f'In some Samples, Taxonomy and scientific names are inconsistent. TaxId - {correct_taxid_sc_name.keys()}')
            self.correct_taxid_scientific_name_in_metadata(correct_taxid_sc_name, self.metadata_file, self.metadata['Sample'])

    def check_biosamples_accessions(self):
        """Check that BioSample accessions exist and are public"""
        for row in self.metadata['Sample']:
            if row.get('Sample Accession'):
                sample_accession = row.get('Sample Accession').strip()
                try:
                    sample_data = self.communicator.follows_link('samples', join_url=sample_accession)
                    self._validate_existing_biosample(sample_data, row.get('row_num'), sample_accession)
                except ValueError:
                    self.error_list.append(
                        f'In Sample, row {row.get("row_num")} BioSamples accession {sample_accession} '
                        f'does not exist or is private')

    def check_project_accessions(self):
        """Check that ENA project accessions exists and are public"""
        for project_row in self.metadata['Project']:
            for column_name in ['Parent Project(s)', 'Child Project(s)', 'Peer Project(s)']:
                if project_row.get(column_name):
                    for project_acc in project_row[column_name].split(','):
                        if not check_project_format(project_acc):
                            self.error_list.append(
                                f'In Project, row {project_row.get("row_num")}, {column_name}: {project_acc} is not a valid project accession')
                            continue
                        if not check_existing_project_in_ena(str(project_acc)):
                            self.error_list.append(
                                f'In Project, row {project_row.get("row_num")}, {column_name}: {project_acc} does not exist or is private')

            column_name = 'Project Alias'
            if project_row.get(column_name):
                project_acc = project_row.get(column_name)
                if check_project_format(project_acc) and not check_existing_project_in_ena(str(project_acc)):
                    self.error_list.append(
                        f'In Project, row {project_row.get("row_num")}, {column_name}: {project_acc} does not exist or is private')

    def correct_taxid_scientific_name_in_metadata(self, correct_taxid_sc_name, metadata_file, samples):
        eva_xls_writer = EvaXlsxWriter(metadata_file)
        samples_to_be_corrected = [sample for sample in samples if sample['Tax Id'] in correct_taxid_sc_name and
                         sample['Scientific Name'] != correct_taxid_sc_name[sample['Tax Id']] and
                                   sample['Scientific Name'].lower() == correct_taxid_sc_name[sample['Tax Id']].lower()]
        for sample in samples_to_be_corrected:
           sample['Scientific Name'] = correct_taxid_sc_name[sample['Tax Id']]

        eva_xls_writer.update_samples(samples_to_be_corrected)
        eva_xls_writer.save()

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

    def _check_date(self, date):
        return isinstance(date, datetime.date) or \
               isinstance(date, datetime.datetime) or \
               self._check_date_str_format(date) or \
               str(date).lower() in not_provided_check_list

    def check_date(self, row, key, required=True):
        if required and not row.get(key):
            self.error_list.append(f'In row {row.get("row_num")}, {key} is required and missing')
            return
        if key in row and self._check_date(row[key]):
            return
        self.error_list.append(f'In row {row.get("row_num")}, {key} is not a date or "not provided": '
                               f'it is set to "{row.get(key)}"')

    def _check_date_str_format(self, d):
        try:
            datetime.datetime.strptime(d, "%Y-%m-%d")
            return True
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(d, "%Y-%m")
            return True
        except ValueError:
            pass
        try:
            datetime.datetime.strptime(d, "%Y")
            return True
        except ValueError:
            return False

    def _validate_existing_biosample(self, sample_data, row_num, accession):
        """This function only check if the existing sample has the expected fields present"""
        found_collection_date=False
        for key in ['collection_date', 'collection date']:
            if key in sample_data['characteristics'] and \
                    self._check_date(sample_data['characteristics'][key][0]['text']):
                found_collection_date = True
        if not found_collection_date:
            self.error_list.append(
                f'In row {row_num}, existing sample accession {accession} does not have a valid collection date')
        found_geo_loc = False
        for key in ['geographic location (country and/or sea)']:
            if key in sample_data['characteristics'] and sample_data['characteristics'][key][0]['text']:
                found_geo_loc = True
        if not found_geo_loc:
            self.error_list.append(
                f'In row {row_num}, existing sample accession {accession} does not have a valid geographic location')
