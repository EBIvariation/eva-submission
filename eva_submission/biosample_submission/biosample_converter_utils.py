from copy import deepcopy

sample_mapping = {
    "uniqueName": "unique name",
    "derivedFrom": "derived from",
    "scientificName": "scientific name",
    "commonName": "common name",
    "matingType": "mating_type",
    "cellType": "cell_type",
    "devStage": "dev_stage",
    "tissueLib": "tissue_lib",
    "tissueType": "tissue_type",
    "BioMaterial": "bio_material",
    "cultureCollection": "culture_collection",
    "specimenVoucher": "specimen_voucher",
    "collectedBy": "collected_by",
    "collectionDate": "collection date",
    "geographicLocationCountrySea": "geographic location (country and/or sea)",
    "geographicLocationRegion": "geographic location (region and locality)",
    "identifiedBy": "identified_by",
    "isolationSource": "isolation_source",
    "latLon": "lat_lon",
    "LabHost": "lab_host",
    "environmentalSample": "environmental_sample",
    "subSpecies": "sub_species",
    "subStrain": "sub_strain",
    "cellLine": "cell_line"
}

def convert_sample(biosample_data):
    '''
    This function will go over several biosample characteristics that have been encoded in camel case by eva-sub-cli prior to
    https://github.com/EBIvariation/eva-sub-cli/releases/tag/v0.4.13 and turn them back to what ERC00011 expects or what was used before in EVA
    '''
    modified_biosample_data = deepcopy(biosample_data)

    for property_name in biosample_data['bioSampleObject']['characteristics']:

        if property_name in sample_mapping and sample_mapping.get(property_name) not in biosample_data['bioSampleObject']['characteristics']:
            modified_biosample_data['bioSampleObject']['characteristics'][sample_mapping.get(property_name)] =  biosample_data['bioSampleObject']['characteristics'][property_name]
            del modified_biosample_data['bioSampleObject']['characteristics'][property_name]
    return modified_biosample_data
