import json

import requests
from ebi_eva_common_pyutils.config import cfg
from retry import retry

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


def sub_ws_auth():
    return (
        cfg.query('submissions', 'webservice', 'admin_username'),
        cfg.query('submissions', 'webservice', 'admin_password')
    )


def sub_ws_url_build(*args, **kwargs):
    url = cfg.query('submissions', 'webservice', 'url') + '/' + '/'.join(args)
    if kwargs:
        return url + '?' + '&'.join(f'{k}={v}' for k, v in kwargs.items())
    else:
        return url


@retry(tries=5, backoff=2, jitter=.5)
def get_from_sub_ws(url):
    response = requests.get(url, auth=sub_ws_auth())
    response.raise_for_status()
    return response.json()


@retry(tries=5, backoff=2, jitter=.5)
def put_to_sub_ws(url):
    response = requests.put(url, auth=sub_ws_auth())
    response.raise_for_status()
    return response.json()


def get_metadata_json_for_submission_id(submission_id):
    submission_details_url = sub_ws_url_build("admin", "submission", submission_id)
    response_data = get_from_sub_ws(submission_details_url)
    metadata_json_data = response_data.get('metadataJson', {})
    if metadata_json_data:
        return metadata_json_data
    else:
        raise ValueError("Metadata json retrieval: missing metadata_json field in the response")


def download_metadata_json_file_for_submission_id(submission_id, metadata_json_file_path):
    metadata_json_data = get_metadata_json_for_submission_id(submission_id)
    with open(metadata_json_file_path, "w", encoding="utf-8") as file:
        json.dump(metadata_json_data, file, indent=4)
