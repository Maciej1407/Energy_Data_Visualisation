"""
GB settlement-period arithmetic.

A BMRS settlement day runs on UTC, but the view we want is a local
(Europe/Berlin) calendar day. In CE(S)T that day is made of settlement periods
47-48 from the previous UTC settlement date followed by periods 1-46 from the
selected one. Every module that needs that rule gets it from here.
"""

import datetime as dt

import pandas as pd

# Periods carried over from the previous UTC settlement date.
carry_over_periods = [47, 48]

# Periods taken from the selected UTC settlement date.
same_day_periods = list(range(1, 47))

date_format = "%Y-%m-%d"


def parse_date(date):
    """
    Parse a 'YYYY-MM-DD' string into a date, passing dates through untouched.
    """
    if isinstance(date, dt.date) and not isinstance(date, dt.datetime):
        return date
    if isinstance(date, dt.datetime):
        return date.date()
    return dt.datetime.strptime(date, date_format).date()


def previous_date(date):
    """Return the settlement date before `date`, as a 'YYYY-MM-DD' string."""
    return (parse_date(date) - dt.timedelta(days=1)).strftime(date_format)


def next_date(date):
    """Return the settlement date after `date`, as a 'YYYY-MM-DD' string."""
    return (parse_date(date) + dt.timedelta(days=1)).strftime(date_format)


def local_day_windows(date):
    """
    Describe the two upstream queries that together make one local day.

    Parameters
    ----------
    date : str or datetime.date
        The UTC settlement date `D` the local day is anchored on.

    Returns
    -------
    list of (str, list of int)
        [(D-1, [47, 48]), (D, [1..46])] - settlement date and the periods to
        take from it.
    """
    day = parse_date(date).strftime(date_format)
    return [
        (previous_date(date), list(carry_over_periods)),
        (day, list(same_day_periods)),
    ]


def period_order():
    """
    Settlement periods in local-day order, as strings: 47, 48, 1 .. 46.

    Returned as strings because plotly treats the axis as categorical, which is
    what stops it re-sorting 47 and 48 back to the end.
    """
    return [str(sp) for sp in (carry_over_periods + same_day_periods)]


def add_period_labels(df, column="settlement_period"):
    """
    Add the categorical label and sort key used by every settlement-period plot.

    Adds `settlement_period_str` and orders the frame by local-day position, so
    that callers never have to remember that 47 and 48 come first.
    """
    df = df.copy()

    if column not in df.columns:
        raise KeyError(f"Expected '{column}' column, got: {list(df.columns)}")

    order = period_order()
    position = {sp: i for i, sp in enumerate(order)}

    df[column] = df[column].astype(int)
    df["settlement_period_str"] = df[column].astype(str)
    df["period_position"] = df["settlement_period_str"].map(position).fillna(len(order))

    df = df.sort_values("period_position").reset_index(drop=True)
    return df


def to_local(series_utc, timezone):
    """
    Convert a UTC timestamp series into `timezone`.

    Parameters
    ----------
    series_utc : pandas.Series
        Timestamps, tz-aware or naive-but-UTC.
    timezone : str
        IANA zone name, e.g. 'Europe/Berlin'.
    """
    return pd.to_datetime(series_utc, utc=True).dt.tz_convert(timezone)
