"""
Parquet round-trip for normalised frames.

The point of writing Parquet is that the schema, not the Python, is the thing
worth keeping: a file written here carries the contract in
energyviz.schema, so a reader in another language reads the same columns with
the same meanings.

Layout on disk is one file per dataset, zone and settlement date:

    {root}/{dataset}/{zone}/{settlement_date}.parquet

which makes re-fetching a single day a single overwrite, and makes a whole
dataset readable by globbing.
"""

import logging
import pathlib

import pandas as pd

from .. import schema

log = logging.getLogger(__name__)

extension = ".parquet"


def path_for(root, dataset, zone, settlement_date):
    """Build the on-disk path for one dataset / zone / settlement date."""
    return pathlib.Path(root) / dataset / zone / f"{settlement_date}{extension}"


def write(frame, root):
    """
    Write a normalised frame, split into one file per dataset/zone/date.

    Parameters
    ----------
    frame : pandas.DataFrame
        A normalised frame; validated before anything is written.
    root : str or pathlib.Path
        Directory to write the tree under. Created if absent.

    Returns
    -------
    list of pathlib.Path
        The files written, in the order they were written.
    """
    schema.validate(frame)

    written = []
    keys = ["dataset", "zone", "settlement_date"]

    for (dataset, zone, settlement_date), part in frame.groupby(keys, sort=True):
        path = path_for(root, dataset, zone, settlement_date)
        path.parent.mkdir(parents=True, exist_ok=True)

        part[schema.columns].reset_index(drop=True).to_parquet(path, index=False)

        written.append(path)
        log.info(f"Saved Parquet: {path} ({len(part)} rows)")

    return written


def read(root, dataset=None, zone=None, settlement_date=None):
    """
    Read back part of the tree as one normalised frame.

    Each of `dataset`, `zone` and `settlement_date` narrows the glob; leaving
    them all out reads everything under `root`.

    Raises
    ------
    FileNotFoundError
        If the pattern matches no files.
    """
    pattern = "/".join([
        dataset or "*",
        zone or "*",
        f"{settlement_date or '*'}{extension}",
    ])

    paths = sorted(pathlib.Path(root).glob(pattern))
    if not paths:
        raise FileNotFoundError(f"No Parquet files under {root} matching {pattern}")

    frame = pd.concat([pd.read_parquet(p) for p in paths], ignore_index=True)
    return schema.conform(frame)
