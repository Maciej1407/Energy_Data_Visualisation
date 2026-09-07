"""
The one HTTP retry loop in the package.

Public data APIs rate-limit, so every request goes through here: a bounded
number of attempts with a short pause between them, and a typed error at the
end rather than a printed complaint. The sleep function is injected so that
tests do not have to wait.
"""

import logging
import time

import requests as rq

from .errors import FetchError

log = logging.getLogger(__name__)


class HttpClient:
    """
    A thin, retrying JSON client.

    Parameters
    ----------
    settings : energyviz.config.Settings
        Supplies the retry policy and the request timeout.
    session : requests.Session, optional
        Injected so callers can share connections or stub the network out.
    sleep : callable, optional
        Injected so tests can run the retry loop instantly.
    """

    def __init__(self, settings, session=None, sleep=time.sleep):
        self.settings = settings
        self.session = session if session is not None else rq.Session()
        self.sleep = sleep

    def get_json(self, url, params, label="request"):
        """
        GET a URL and return the decoded JSON body.

        Retries up to `settings.retry.attempts` times on a transport error or a
        non-200 status, pausing `settings.retry.delay_seconds` between tries.

        Parameters
        ----------
        url : str
            Fully qualified URL.
        params : dict
            Query parameters.
        label : str
            Human-readable name for this call, used in log lines and in the
            error message if every attempt fails.

        Raises
        ------
        FetchError
            If no attempt came back with a 200.
        """
        policy = self.settings.retry
        attempt = 1
        last_problem = "no attempt was made"

        while attempt <= policy.attempts:
            try:
                log.info(f"{label} attempt {attempt} ...")
                r = self.session.get(url, params=params, timeout=self.settings.request_timeout)

                if r.status_code == 200:
                    log.info(f"{label} OK.")
                    return r.json()

                last_problem = f"HTTP status {r.status_code}"
                log.warning(f"{label} {last_problem}")
            except Exception as e:
                last_problem = str(e)
                log.warning(f"{label} attempt {attempt} failed: {e}. Retrying...")

            attempt += 1
            if attempt <= policy.attempts:
                self.sleep(policy.delay_seconds)

        raise FetchError(
            f"{label} failed after {policy.attempts} attempts; last problem: {last_problem}"
        )

    def space(self):
        """Pause between two back-to-back successful calls, per the retry policy."""
        self.sleep(self.settings.retry.spacing_seconds)
