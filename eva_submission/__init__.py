import os
import eva_submission


ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # This is your Project Root

NEXTFLOW_DIR = os.path.join(os.path.dirname(os.path.abspath(eva_submission.__file__)), 'nextflow')
ETC_DIR = os.path.join(os.path.dirname(os.path.abspath(eva_submission.__file__)), 'etc')
