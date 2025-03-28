import json
from datetime import datetime

import openpyxl
from openpyxl.workbook import Workbook

json_to_xlsx_key_mapper = {
    "worksheets": {
        "submitterDetails": "Submitter Details",
        "project": "Project",
        "analysis": "Analysis",
        "sample": "Sample",
        "files": "Files"
    },

    "submitterDetails": {
        "lastName": "Last Name",
        "firstName": "First Name",
        "email": "Email Address",
        "laboratory": "Laboratory",
        "centre": "Center",
        "address": "Address",
        'DUMMY1': 'Telephone Number'
    },

    "project": {
        "title": "Project Title",
        "description": "Description",
        "centre": "Center",
        "taxId": "Tax ID",
        "publications": "Publication(s)",
        "parentProject": "Parent Project",
        "childProjects": "Child Project(s)",
        "peerProjects": "Peer Project(s)",
        "links": "Link(s)",
        "holdDate": "Hold Date",
        "collaborators": "Collaborator(s)",
        "strain": "Strain",
        "breed": "Breed",
        "broker": "Broker",
        'DUMMY1': 'Project Alias'
    },

    "analysis": {
        "analysisTitle": "Analysis Title",
        "analysisAlias": "Analysis Alias",
        "description": "Description",
        "experimentType": "Experiment Type",
        "referenceGenome": "Reference",
        "referenceFasta": "Reference Fasta Path",
        "platform": "Platform",
        "software": "Software",
        "pipelineDescriptions": "Pipeline Description",
        "imputation": "Imputation",
        "phasing": "Phasing",
        "centre": "Centre",
        "date": "Date",
        "links": "Link(s)",
        "runAccessions": "Run Accession(s)",
        'DUMMY1': 'Project Title'
    },

    "sample": {
        "analysisAlias": "Analysis Alias",
        "sampleInVCF": "Sample Name in VCF",
        "bioSampleAccession": "Sample Accession",
        "bioSampleName": "BioSample Name",
        "title": "Title",
        "description": "Description",
        "uniqueName": "Unique Name",
        "prefix": "Prefix",
        "subject": "Subject",
        "derivedFrom": "Derived From",
        "taxId": "Tax Id",
        "scientificName": "Scientific Name",
        "commonName": "Common Name",
        "matingType": "mating_type",
        "sex": "sex",
        "population": "population",
        "cellType": "cell_type",
        "devStage": "dev_stage",
        "germline": "germline",
        "tissueLib": "tissue_lib",
        "tissueType": "tissue_type",
        "BioMaterial": "bio_material",
        "cultureCollection": "culture_collection",
        "specimenVoucher": "specimen_voucher",
        "collectedBy": "collected_by",
        "collectionDate": "collection_date",
        "geographicLocationCountrySea": "geographic location (country and/or sea)",
        "geographicLocationRegion": "geographic location (region and locality)",
        "host": "host",
        "identifiedBy": "identified_by",
        "isolationSource": "isolation_source",
        "latLon": "lat_lon",
        "LabHost": "lab_host",
        "environmentalSample": "environmental_sample",
        "cultivar": "cultivar",
        "ecotype": "ecotype",
        "isolate": "isolate",
        "strain": "strain",
        "subSpecies": "sub_species",
        "variety": "variety",
        "subStrain": "sub_strain",
        "cellType": "cell_line",
        "serotype": "serotype",
        "serovar": "serovar"
    },

    "files": {
        "analysisAlias": "Analysis Alias",
        "fileName": "File Name",
        'DUMMY1': 'MD5',
        'DUMMY2': 'File Type'
    }
}


class JsonToXlsxConverter:

    def convert_json_to_xlsx(self, input_json_file, output_xlsx_file):
        with open(input_json_file, 'r') as f:
            data = json.load(f)

        workbook = Workbook()
        self.create_dummy_instructions_sheet(workbook)
        for worksheet_key in json_to_xlsx_key_mapper['worksheets']:
            worksheet_title = json_to_xlsx_key_mapper['worksheets'][worksheet_key]
            worksheet_data = data[worksheet_key]
            self.create_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)

        workbook.save(output_xlsx_file)

    def create_worksheet(self, workbook, worksheet_key, worksheet_title, worksheet_data):
        self.create_worksheet_header(workbook, worksheet_key, worksheet_title)
        if worksheet_key == "submitterDetails":
            self.create_submitter_details_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)
        elif worksheet_key == "project":
            self.create_project_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)
        elif worksheet_key == "analysis":
            self.create_analysis_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)
        elif worksheet_key == "sample":
            self.create_sample_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)
        elif worksheet_key == "files":
            self.create_file_worksheet(workbook, worksheet_key, worksheet_title, worksheet_data)

    def create_worksheet_header(self, workbook, worksheet_key, worksheet_title):
        workbook.create_sheet(worksheet_title)
        if worksheet_key == "sample":
            header_row_index = 3
        else:
            header_row_index = 1
        header_col_index = 0
        for header_key in sorted(json_to_xlsx_key_mapper[worksheet_key]):
            header_col_index += 1
            cell = workbook[worksheet_title].cell(column=header_col_index, row=header_row_index,
                                                  value=json_to_xlsx_key_mapper[worksheet_key][header_key])
            cell.font = openpyxl.styles.Font(bold=True)

    def create_dummy_instructions_sheet(self, workbook):
        worksheet_title = 'PLEASE READ FIRST'
        workbook.create_sheet(worksheet_title)
        workbook[worksheet_title].cell(column=1, row=3, value='V1.1.4 August 2020')

    def create_submitter_details_worksheet(self, workbook, worksheet_key, worksheet_title, submitter_details):
        row_index = 1
        for submitter in submitter_details:
            row_index += 1
            col_index = 0
            for header_name in sorted(json_to_xlsx_key_mapper[worksheet_key]):
                col_index += 1
                if header_name in submitter:
                    workbook[worksheet_title].cell(column=col_index, row=row_index, value=submitter[header_name])

    def create_project_worksheet(self, workbook, worksheet_key, worksheet_title, project_data):
        row_index = 2
        col_index = 0
        for header_name in sorted(json_to_xlsx_key_mapper[worksheet_key]):
            col_index += 1
            if header_name in project_data:
                cell_value = project_data[header_name]
                if header_name in ['publications', 'childProjects', 'peerProjects', 'links']:
                    cell_value = ",".join(cell_value)
                elif header_name in ['holdDate']:
                    cell_value = datetime.strptime(cell_value, "%Y-%m-%d").date()

                workbook[worksheet_title].cell(column=col_index, row=row_index, value=cell_value)

    def create_analysis_worksheet(self, workbook, worksheet_key, worksheet_title, analysis_data):
        row_index = 1
        for analysis in analysis_data:
            row_index += 1
            col_index = 0
            for header_name in sorted(json_to_xlsx_key_mapper[worksheet_key]):
                col_index += 1
                if header_name in analysis:
                    cell_value = analysis[header_name]
                    if header_name in ['runAccessions']:
                        cell_value = ",".join(cell_value)
                    elif header_name in ['imputation', 'phasing']:
                        cell_value = '1' if cell_value == True else ''
                    elif header_name in ['date']:
                        cell_value = datetime.strptime(cell_value, "%Y-%m-%d").date()

                    workbook[worksheet_title].cell(column=col_index, row=row_index, value=cell_value)

    def create_sample_worksheet(self, workbook, worksheet_key, worksheet_title, sample_data):
        row_index = 3
        for sample in sample_data:
            row_index += 1
            sample_flattened_data = {**sample,
                                     **sample.get('bioSampleObject', {}),
                                     **({'bioSampleName': sample['bioSampleObject']['name']}
                                        if 'bioSampleObject' in sample and 'name' in sample['bioSampleObject'] and
                                           sample['bioSampleObject']['name']
                                        else {}),
                                     **{key: char_data[0].get('text')
                                        for key, char_data in
                                        sample.get('bioSampleObject', {}).get('characteristics', {}).items()
                                        }
                                     }
            col_index = 0
            for header_name in sorted(json_to_xlsx_key_mapper[worksheet_key]):
                col_index += 1
                if header_name in sample_flattened_data:
                    cell_value = sample_flattened_data[header_name]
                    if header_name in ['analysisAlias']:
                        cell_value = ",".join(cell_value)
                    elif header_name in ['collectionDate']:
                        cell_value = datetime.strptime(cell_value, "%Y-%m-%d").date()

                    workbook[worksheet_title].cell(column=col_index, row=row_index, value=cell_value)

    def create_file_worksheet(self, workbook, worksheet_key, worksheet_title, files_data):
        row_index = 1
        for file in files_data:
            row_index += 1
            col_index = 0
            for header_name in sorted(json_to_xlsx_key_mapper[worksheet_key]):
                col_index += 1
                if header_name in file:
                    workbook[worksheet_title].cell(column=col_index, row=row_index, value=file[header_name])
