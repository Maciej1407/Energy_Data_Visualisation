"""
The normalised frame contract.

Every source fetcher ends its job by producing a frame shaped like this, and
every transform, plot and Parquet write starts from one. It is the single thing
the whole package agrees on, and the seam along which a future Go rewrite would
cut: reimplement the fetchers and the writer, keep this schema identical, and
the Parquet files stay interchangeable.

The frame is a long-format *observation log*, not a deduplicated cube. One row
per observation the upstream API reported, so a single (dataset, settlement
date, settlement period, series) may legitimately appear more than once - wind
arrives split into onshore and offshore, and an evolving forecast arrives once
per publish time. Collapsing those is the job of energyviz.domain, not of the
fetcher.
"""

import pandas as pd

from .errors import SchemaError


# =========================
# Column contract
# =========================

columns = [
    "source",             # "elexon", later "entsoe"
    "dataset",            # see datasets below
    "zone",               # bidding / market zone, e.g. "GB"
    "settlement_date",    # date the upstream API files the row under
    "settlement_period",  # 1..48 for GB; NA for sources without periods
    "start_utc",          # start of the delivery interval, UTC
    "start_local",        # same instant in Settings.timezone
    "publish_utc",        # when upstream published it; NA where not reported
    "series",             # see series below
    "value",
    "unit",               # "MW", later "EUR/MWh"
]

dtypes = {
    "source": "object",
    "dataset": "object",
    "zone": "object",
    "settlement_date": "object",
    "settlement_period": "Int64",
    "start_utc": "datetime64[ns, UTC]",
    "publish_utc": "datetime64[ns, UTC]",
    "series": "object",
    "value": "float64",
    "unit": "object",
}


# =========================
# Vocabulary
# =========================

# datasets
imbalance_forecast = "imbalance_forecast"
generation_forecast = "generation_forecast"
generation_actual = "generation_actual"
day_ahead_price = "day_ahead_price"

datasets = [
    imbalance_forecast,
    generation_forecast,
    generation_actual,
    day_ahead_price,
]

# series
imbalance = "imbalance"
generation = "generation"
demand = "demand"
margin = "margin"
wind = "wind"
solar = "solar"
price = "price"

series = [
    imbalance,
    generation,
    demand,
    margin,
    wind,
    solar,
    price,
]

# units
mw = "MW"
eur_mwh = "EUR/MWh"


# =========================
# Construction and checking
# =========================

def empty_frame():
    """
    Build an empty frame carrying the full contract, columns in canonical order.

    Useful as a starting point and as the honest return value when an API
    answers with no rows at all.
    """
    df = pd.DataFrame({col: pd.Series(dtype=dtypes.get(col, "object")) for col in columns})
    return df[columns]


def conform(df):
    """
    Put a frame into canonical column order and dtypes.

    Missing optional columns are filled with NA rather than raising, so that a
    fetcher only has to supply what its API actually reports.

    Parameters
    ----------
    df : pandas.DataFrame
        Frame carrying at least the identifying columns.
    """
    df = df.copy()

    for col in columns:
        if col not in df.columns:
            df[col] = pd.NA

    for col, dtype in dtypes.items():
        try:
            df[col] = df[col].astype(dtype)
        except (TypeError, ValueError) as e:
            raise SchemaError(f"Column '{col}' could not be cast to {dtype}: {e}")

    return df[columns].reset_index(drop=True)


def validate(df):
    """
    Assert that a frame satisfies the contract, and hand it back unchanged.

    Raises
    ------
    SchemaError
        If columns are missing, or if a categorical column carries a value
        outside the vocabulary declared above.
    """
    missing = [col for col in columns if col not in df.columns]
    if missing:
        raise SchemaError(
            f"Frame is missing contract columns {missing}; got: {list(df.columns)}"
        )

    for col, allowed in (("dataset", datasets), ("series", series)):
        unknown = sorted(set(df[col].dropna().unique()) - set(allowed))
        if unknown:
            raise SchemaError(
                f"Column '{col}' carries values outside the vocabulary: {unknown}. "
                f"Allowed: {allowed}"
            )

    return df


def select(df, dataset=None, series_name=None):
    """
    Narrow a normalised frame to one dataset and/or one series.

    Parameters
    ----------
    df : pandas.DataFrame
        A normalised frame.
    dataset : str, optional
        One of `datasets`.
    series_name : str or list of str, optional
        One or more of `series`.
    """
    out = df

    if dataset is not None:
        out = out[out["dataset"] == dataset]

    if series_name is not None:
        wanted = [series_name] if isinstance(series_name, str) else list(series_name)
        out = out[out["series"].isin(wanted)]

    return out.reset_index(drop=True)
