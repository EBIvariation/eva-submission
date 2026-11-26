from datetime import datetime, date

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

def get_or_generate_submission_id_for_eload(eload_id, source='email'):
    response = get_from_sub_ws(sub_ws_url_build('admin', 'submission', str(eload_id), 'submissionId', source=source))
    if not response:
        return None
    return response.get('submissionId')

def update_tracking_details(submission_id, release_date=None, project_accession=None,
                            rt_link=None, metadata_json_file=None, project_title=None, project_taxonomies=None,
                            analyses=None):
    if isinstance(release_date, datetime):
        release_date = release_date.date().isoformat()
    elif isinstance(release_date, date):
        release_date = release_date.isoformat()

    candidates = {
        'releaseDate': release_date,
        'projectAccession': project_accession,
        'rtLink': rt_link,
        'metadataJsonFile': metadata_json_file,
        'projectTitle': project_title,
        'projectTaxonomies': project_taxonomies,
        'analyses': analyses
    }
    body = {key: value for key, value in candidates.items() if value}

    if body:
        try:
            put_to_sub_ws(sub_ws_url_build('admin', 'submission', submission_id, 'trackingDetails'), body)
        except Exception as e:
            logger.warning(f'Could not update submission tracking details for {submission_id}. Error {e}')
    else:
        logger.info(f'No submission tracking details to update for {submission_id}')


def get_processing_state(submission_id):
    try:
        url = sub_ws_url_build('admin', 'submission-process', submission_id)
        response = requests.get(url, auth=sub_ws_auth(), timeout=60)
        response.raise_for_status()
        if response.status_code == 204:
            logger.warning(f'No processing state available for {submission_id}: {response.text}')
            return None, None
        state = response.json() if response.text else {}
        return state.get('step'), state.get('status')
    except Exception as e:
        logger.error(f'Could not retrieve the processing state for {submission_id}. Error {e}')
    return None, None


def get_tracking_details(submission_id):
    try:
        url = sub_ws_url_build('admin', 'submission', str(submission_id), 'trackingDetails')
        response = requests.get(url, auth=sub_ws_auth(), timeout=60)
        response.raise_for_status()
        if not response.text:
            return None
        return response.json()
    except Exception as e:
        logger.warning(f'Could not retrieve submission tracking details for {submission_id}. Error {e}')
        return None


def delete_tracking_details(submission_id):
    try:
        url = sub_ws_url_build('admin', 'submission', str(submission_id), 'trackingDetails')
        response = requests.delete(url, auth=sub_ws_auth(), timeout=60)
        if response.status_code == 404:
            logger.warning(f'No tracking details exists for submission {submission_id}')
            return None
        response.raise_for_status()
    except Exception as e:
        logger.warning(f'Could not delete submission tracking details for {submission_id}. Error {e}')
        return None