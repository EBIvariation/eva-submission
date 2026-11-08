from unittest.mock import patch

import pytest


@pytest.fixture(autouse=True)
def no_submission_ws_tracking_details():
    """
    Keep the tests independent of the submission-ws: by default no tracking details can be retrieved (so the code
    falls back on the config) and updates are no-ops. Tests that care patch these names themselves.
    """
    with patch('eva_submission.submission.get_tracking_details', return_value=None), \
            patch('eva_submission.submission.update_tracking_details'):
        yield