"""
Wind and solar: line up the day-ahead forecast against what actually turned up.

Pure transforms on normalised frames - no network, no files, no clock.
"""

from dataclasses import dataclass

from .. import schema
from . import settlement

group_columns = ["settlement_date", "settlement_period", "series"]


def align(forecast, actual):
    """
    Aggregate and merge a forecast frame with an actuals frame.

    Wind arrives split into onshore and offshore, so both sides are summed per
    (settlement date, period, series) before the join. The join is inner: a
    period is only shown once both a forecast and an outturn exist for it.

    Parameters
    ----------
    forecast, actual : pandas.DataFrame
        Normalised frames carrying `generation_forecast` and
        `generation_actual` respectively.

    Returns
    -------
    pandas.DataFrame
        One row per (settlement date, period, series), carrying `forecast_mw`,
        `actual_mw`, `diff_mw` and the local start time.
    """
    forecast_agg = _aggregate(forecast, schema.generation_forecast, "forecast_mw")
    actual_agg = _aggregate(actual, schema.generation_actual, "actual_mw")

    merged = forecast_agg.merge(
        actual_agg,
        on=group_columns,
        how="inner",
        suffixes=("_forecast", "_actual"),
    )

    merged["start_local"] = merged["start_local_forecast"].combine_first(
        merged["start_local_actual"]
    )
    merged["diff_mw"] = merged["actual_mw"] - merged["forecast_mw"]

    merged = merged.sort_values(["settlement_date", "series", "settlement_period"])
    return merged.reset_index(drop=True)


def _aggregate(frame, dataset, value_name):
    df = schema.select(frame, dataset=dataset)

    if df.empty:
        raise ValueError(f"No rows for dataset '{dataset}'.")

    return (
        df
        .groupby(group_columns, as_index=False)
        .agg({"value": "sum", "start_local": "min"})
        .rename(columns={"value": value_name})
    )


def split(aligned, series_name):
    """
    Take one series out of an aligned frame, in local-day period order.

    Parameters
    ----------
    aligned : pandas.DataFrame
        Output of `align`.
    series_name : str
        `schema.wind` or `schema.solar`.
    """
    df = aligned[aligned["series"] == series_name].copy()

    if df.empty:
        return df.reset_index(drop=True)

    return settlement.add_period_labels(df)


@dataclass(frozen=True)
class ErrorSummary:
    """
    Forecast error statistics for one series, all in MW.

    Signed as actual minus forecast, so a positive mean error means the system
    generated more than the day-ahead forecast expected.
    """

    label: str
    mean_error: float
    mean_absolute_error: float
    worst_under_forecast: float
    worst_over_forecast: float

    def lines(self):
        """Render as the lines the CLI prints, one string per statistic."""
        return [
            f"{self.label} mean error (actual - forecast): {self.mean_error:.1f} MW",
            f"{self.label} mean absolute error:           {self.mean_absolute_error:.1f} MW",
            f"{self.label} max under-forecast:            {self.worst_under_forecast:.1f} MW",
            f"{self.label} max over-forecast:             {self.worst_over_forecast:.1f} MW",
        ]


def error_summary(df, label):
    """
    Compute forecast error statistics for one aligned series.

    Returns None for an empty frame rather than raising, so that a day with no
    solar outturn does not stop the run.
    """
    if df.empty:
        return None

    diffs = df["diff_mw"].astype(float)

    return ErrorSummary(
        label=label,
        mean_error=diffs.mean(),
        mean_absolute_error=diffs.abs().mean(),
        worst_under_forecast=diffs.min(),
        worst_over_forecast=diffs.max(),
    )
