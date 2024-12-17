from eva_sub_cli_processing.sub_cli_submission import SubCliProcess


class SubCliProcessValidation(SubCliProcess):

    all_validation_tasks = ['metadata_check', 'assembly_check', 'aggregation_check', 'vcf_check', 'sample_check',
                            'structural_variant_check', 'naming_convention_check']

    def start(self):
        pass

