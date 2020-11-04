from distutils.core import setup

from setuptools import find_packages

setup(
    name='eva_submission',
    packages=find_packages(),
    version='0.0.1',
    license='Apache',
    description='EBI EVA - submission processing tools',
    url='https://github.com/EBIVariation/eva-submission',
    keywords=['ebi', 'eva', 'python', 'submission'],
    install_requires=['openpyxl', 'pyyaml', 'cached-property', 'humanize', 'pysam', 'ebi_eva_common_pyutils'],
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Build Tools',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3'
    ],
    scripts=['bin/detect_submission.py', 'bin/check_samples_eva.py', 'bin/genome_downloader.py',
             'bin/prepare_submission.py']
)
