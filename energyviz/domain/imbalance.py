"""
Indicated imbalance: pick the live forecast, label it, and compare snapshots.

Pure transforms on normalised frames - no network, no files, no clock.
"""

import pandas as pd

from .. import schema
from . import settlement

positive = "Positive"
negative = "Negative"


def latest_per_period(frame, series_name=schema.imbalance):
    """
    Reduce an evolving forecast to one row per settlement period.

    The BMRS evolution feed republishes a period every time the forecast moves,
    so a local day arrives as thousands of rows. Rows with no value are dropped
    first, then the most recently published row per period survives.

    Parameters
    ----------
    frame : pandas.DataFrame
        A normalised frame (see energyviz.schema).
    series_name : str
        Which series to reduce; defaults to imbalance.
    """
    df = schema.select(frame, dataset=schema.imbalance_forecast, series_name=series_name)

    df = df.dropna(subset=["value"]).copy()
    df = df.sort_values("publish_utc")

    latest = (
        df
        .groupby(["settlement_date", "settlement_period", "series"])
        .tail(1)
        .reset_index(drop=True)
    )

    return settlement.add_period_labels(latest)


def add_sign(df, column="value"):
    """
    Label each row Positive or Negative, for colouring.

    Zero counts as positive, matching how the surplus/deficit split is read.
    """
    df = df.copy()

    if column not in df.columns:
        raise KeyError(f"Expected '{column}' column, got: {list(df.columns)}")

    df[f"{column}_sign"] = df[column].apply(
        lambda x: positive if pd.notna(x) and x >= 0
        else negative if pd.notna(x)
        else None
    )

    return df


def latest_publish(df):
    """Most recent publish timestamp in a frame, or None if it carries none."""
    if df.empty or df["publish_utc"].isna().all():
        return None
    return df["publish_utc"].max()


def diff_snapshots(previous, latest):
    """
    Align two snapshots of the same forecast and measure how far each period moved.

    Snapshots of the same settlement date are matched on (date, period); if the
    two snapshots cover different dates they are matched on period alone, so
    that one day can be laid over another.

    Parameters
    ----------
    previous, latest : pandas.DataFrame
        Frames as returned by `latest_per_period`.

    Returns
    -------
    (pandas.DataFrame, bool)
        The merged frame carrying `value_prev`, `value_new`, `delta` and a sign
        column for each side, and whether the two snapshots share a date.
    """
    previous = previous.copy()
    latest = latest.copy()

    previous_dates = previous["settlement_date"].unique()
    latest_dates = latest["settlement_date"].unique()

    same_date = (
        len(previous_dates) == 1
        and len(latest_dates) == 1
        and previous_dates[0] == latest_dates[0]
    )
    merge_on = ["settlement_date", "settlement_period"] if same_date else ["settlement_period"]

    merged = previous.merge(
        latest,
        on=merge_on,
        how="outer",
        suffixes=("_prev", "_new"),
    )

    merged["delta"] = merged["value_new"] - merged["value_prev"]

    for side in ("prev", "new"):
        merged = add_sign(merged, column=f"value_{side}")

    merged = settlement.add_period_labels(merged)

    return merged, same_date
