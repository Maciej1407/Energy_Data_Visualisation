"""
Runtime settings for energyviz.

Everything the library needs to know about *where* data comes from and *how* to
talk to it lives here, so that no module reaches for a global or an environment
variable on its own. Settings are constructed once at the CLI boundary and
passed down explicitly.
"""

from dataclasses import dataclass, field


# =========================
# Defaults
# =========================

elexon_base_url = "https://data.elexon.co.uk/bmrs/api/v1"
local_timezone = "Europe/Berlin"


@dataclass(frozen=True)
class RetryPolicy:
    """
    How hard to try a request before giving up.

    Parameters
    ----------
    attempts : int
        Total number of attempts, including the first (default 5).
    delay_seconds : float
        Pause between attempts, to stay inside public API rate limits.
    spacing_seconds : float
        Pause between two *successful* back-to-back calls in the same fetch.
    """

    attempts: int = 5
    delay_seconds: float = 2.0
    spacing_seconds: float = 1.0


@dataclass(frozen=True)
class Settings:
    """
    Everything a source client needs in order to run.

    Parameters
    ----------
    base_url : str
        Root of the REST API to query.
    timezone : str
        IANA zone used to build the *local* day view (default Europe/Berlin).
    retry : RetryPolicy
        Retry behaviour applied to every request.
    request_timeout : float
        Per-request socket timeout in seconds.
    """

    base_url: str = elexon_base_url
    timezone: str = local_timezone
    retry: RetryPolicy = field(default_factory=RetryPolicy)
    request_timeout: float = 60.0
