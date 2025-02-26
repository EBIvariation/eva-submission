import glob
from distutils.core import setup
from os.path import join, abspath, dirname

base_dir = abspath(dirname(__file__))
requirements_txt = join(base_dir, 'requirements.txt')
requirements = [l.strip() for l in open(requirements_txt) if l and not l.startswith('#')]

version = open(join(base_dir, 'eva_submission', 'VERSION')).read().strip()

setup(
    name='eva_submission',
    packages=['eva_submission', 'eva_submission.ENA_submission', 'eva_submission.xlsx', 'eva_submission.steps',
              'eva_submission.biosample_submission', 'eva_sub_cli_processing',
              'eva_sub_cli_processing.sub_cli_to_eload_converter'],
    package_data={'eva_submission': ['nextflow/*', 'etc/*', 'VERSION']},
    version=version,
    license='Apache',
    description='EBI EVA - submission processing tools',
    long_description='This package contains tools used to validated broker and ingest the submission received by the '
                     'EVA. It is mean to be used by the EVA team and is not mean to be used externally',
    url='https://github.com/EBIVariation/eva-submission',
    keywords=['ebi', 'eva', 'python', 'submission'],
    install_requires=requirements,
    python_requires='>=3.8',
    author='The EVA team',
    author_email='eva-dev@ebi.ac.uk',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9'
    ],
    scripts=glob.glob(join(dirname(__file__), 'bin', '*.py'))
)
