"""Aligning forecast with outturn, and scoring the error."""

import pandas as pd
import pytest

from energyviz import schema
from energyviz.domain import windsolar


def test_align_gives_one_row_per_period_and_fuel(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)

    assert len(aligned) == 96
    assert set(aligned["series"]) == {schema.wind, schema.solar}
    assert not aligned.duplicated(windsolar.group_columns).any()


def test_onshore_and_offshore_are_summed_into_one_wind_number(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)
    row = aligned[
        (aligned["series"] == schema.wind)
        & (aligned["settlement_date"] == "2025-11-11")
        & (aligned["settlement_period"] == 20)
    ].iloc[0]

    raw = forecast_frame[
        (forecast_frame["series"] == schema.wind)
        & (forecast_frame["settlement_date"] == "2025-11-11")
        & (forecast_frame["settlement_period"] == 20)
    ]

    assert len(raw) == 2
    assert row["forecast_mw"] == pytest.approx(raw["value"].sum())


def test_diff_is_actual_minus_forecast(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)
    expected = aligned["actual_mw"] - aligned["forecast_mw"]

    assert aligned["diff_mw"].round(6).equals(expected.round(6))


def test_split_returns_one_fuel_in_local_day_order(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)
    wind = windsolar.split(aligned, schema.wind)

    assert len(wind) == 48
    assert wind["settlement_period_str"].iloc[0] == "47"


def test_align_refuses_a_frame_with_no_rows_for_the_dataset(forecast_frame):
    with pytest.raises(ValueError, match="generation_actual"):
        windsolar.align(forecast_frame, forecast_frame)


def test_error_summary_reads_actual_minus_forecast():
    df = pd.DataFrame({"diff_mw": [10.0, -30.0, 20.0]})
    summary = windsolar.error_summary(df, "Wind")

    assert summary.mean_error == pytest.approx(0.0)
    assert summary.mean_absolute_error == pytest.approx(20.0)
    assert summary.worst_under_forecast == -30.0
    assert summary.worst_over_forecast == 20.0
    assert summary.lines()[0].startswith("Wind mean error")


def test_error_summary_of_an_empty_frame_is_none():
    assert windsolar.error_summary(pd.DataFrame({"diff_mw": []}), "Solar") is None
