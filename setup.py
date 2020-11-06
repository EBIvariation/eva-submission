import glob
from distutils.core import setup
from os.path import join, abspath, dirname

from setuptools import find_packages

requirements_txt = join(abspath(dirname(__file__)), 'requirements.txt')
requirements = [l.strip() for l in open(requirements_txt) if l and not l.startswith('#')]

setup(
    name='eva_submission',
    packages=['eva_submission'],
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
    scripts=glob.glob(join(dirname(__file__), 'bin', '*.py'))
)
