import requests
from ebi_eva_common_pyutils.config import cfg
from ebi_eva_common_pyutils.logger import logging_config as log_cfg
from retry import retry

logger = log_cfg.get_logger(__name__)

# Submission statuses
OPEN = 'OPEN'
UPLOADED = 'UPLOADED'
COMPLETED = 'COMPLETED'
TIMEOUT = 'TIMEOUT'
FAILED = 'FAILED'
CANCELLED = 'CANCELLED'
PROCESSING = 'PROCESSING'

# Processing steps
VALIDATION = 'VALIDATION'
BROKERING = 'BROKERING'
INGESTION = 'INGESTION'
PROCESSING_STEPS = [VALIDATION, BROKERING, INGESTION]

# Processing statuses
READY_FOR_PROCESSING = 'READY_FOR_PROCESSING'
FAILURE = 'FAILURE'
SUCCESS = 'SUCCESS'
RUNNING = 'RUNNING'
ON_HOLD = 'ON_HOLD'
PROCESSING_STATUS = [READY_FOR_PROCESSING, FAILURE, SUCCESS, RUNNING, ON_HOLD]
SUBMISSION_STATUS = [OPEN, UPLOADED, COMPLETED, TIMEOUT, FAILED, CANCELLED, PROCESSING]


def sub_ws_auth():
    return (
        cfg.query('submissions', 'webservice', 'admin_username'),
        cfg.query('submissions', 'webservice', 'admin_password')
    )


def sub_ws_url_build(*args, **kwargs):
    url = cfg.query('submissions', 'webservice', 'url') + '/' + '/'.join(args)
    if kwargs:
        query_params = []
        for k, v in kwargs.items():
            if isinstance(v, list):
                query_params.extend([f'{k}={v2}' for v2 in v])
            else:
                query_params.append(f'{k}={v}')
        return url + '?' + '&'.join(query_params)
    else:
        return url


@retry(tries=5, backoff=2, jitter=.5)
def get_from_sub_ws(url):
    response = requests.get(url, auth=sub_ws_auth())
    response.raise_for_status()
    return response.json()


@retry(tries=5, backoff=2, jitter=.5)
def put_to_sub_ws(url, json_data=None):
    response = requests.put(url, auth=sub_ws_auth(), json=json_data)
    response.raise_for_status()
    if not response.text:
        return None
    return response.json()


def post_to_sub_ws(url, json_data=None):
    response = requests.post(url, auth=sub_ws_auth(), json=json_data)
    response.raise_for_status()
    if not response.text:
        return None

    content_type = response.headers.get('Content-Type', '')
    if 'application/json' in content_type:
        return response.json()

    try:
        return response.json()
    except ValueError:
        return response.text.strip()


def fetch_submission_from_eload(eload_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', eloadId=eload_id, size=1))
    content = response.get('content', [])
    if not content:
        return None
    return content[0]


def fetch_submission(submission_id):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submissions', submissionId=submission_id, size=1))
    content = response.get('content', [])
    if not content:
        return None
    return content[0]

def initiate_eva_submission(identifier):
    response = post_to_sub_ws(sub_ws_url_build('admin', 'submission', "initiate", str(identifier), size=1))
    if not response:
        return None
    return response

def update_tracking_details(submission_id, release_date=None, project_accession=None, analysis_accessions=None,
                            rt_link=None):
    body = {}
    if release_date:
        body['releaseDate'] = release_date
    if project_accession:
        body['projectAccession'] = project_accession
    if analysis_accessions:
        body['analysisAccessions'] = analysis_accessions
    if rt_link:
        body['rtLink'] = rt_link

    if body:
        try:
            put_to_sub_ws(sub_ws_url_build('admin', 'submission', submission_id, 'trackingDetails'), body)
        except Exception as e:
            logger.warning(f'Could not update submission tracking details for {submission_id}. Error {e}')
    else:
        logger.info('No submission tracking details to update for {submission_id}')
