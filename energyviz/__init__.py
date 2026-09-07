"""
energyviz - fetch, normalise, and plot GB electricity market data.

The package is layered so that each piece can be used on its own:

    sources/  talk to an API and return a normalised frame (see schema.py)
    domain/   pure transforms on normalised frames - no I/O
    viz/      normalised frame -> plotly Figure, and the one place we write files
    store/    Parquet round-trip
    watch     poll an upstream feed until it publishes something new

Nothing below the cli/ scripts prints, writes, or reads the clock on its own;
those are passed in.
"""

from . import config, errors, schema

__all__ = ["config", "errors", "schema"]
