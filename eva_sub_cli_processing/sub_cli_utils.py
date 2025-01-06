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
