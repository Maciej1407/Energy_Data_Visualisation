"""
Source registry.

One entry per upstream API. Adding a source means writing its module and adding
a line here; nothing else in the package needs to know it exists.
"""

from ..transport import HttpClient
from . import elexon

builders = {
    elexon.source_name: elexon.ElexonClient,
}


def build(name, settings, session=None, sleep=None):
    """
    Construct a source client by name.

    Parameters
    ----------
    name : str
        A key of `builders`, e.g. 'elexon'.
    settings : energyviz.config.Settings
    session : requests.Session, optional
    sleep : callable, optional
        Injected into the HTTP client's retry loop.

    Raises
    ------
    KeyError
        If the source is not registered.
    """
    builder = builders.get(name)
    if builder is None:
        raise KeyError(f"Unknown source '{name}'; known: {sorted(builders)}")

    kwargs = {} if sleep is None else {"sleep": sleep}
    return builder(HttpClient(settings, session=session, **kwargs), settings)
