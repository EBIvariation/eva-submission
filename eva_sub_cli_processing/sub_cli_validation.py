from eva_sub_cli_processing.sub_cli_submission import SubCli


class SubCliValidation(SubCli):

    all_validation_tasks = ['metadata_check', 'assembly_check', 'aggregation_check', 'vcf_check', 'sample_check',
                            'structural_variant_check', 'naming_convention_check']

    def validate(self):
        pass

