"""
Regression against the pre-refactor scripts.

The CSVs in tests/golden were produced by running task1.py and task2.py as they
stood before the refactor, over the payloads in tests/fixtures. If a change
here moves a number, this is what says so.
"""

import pathlib

import pandas as pd
import pytest

from energyviz import schema
from energyviz.domain import imbalance, windsolar

golden_dir = pathlib.Path(__file__).parent / "golden"


def golden(name):
    return pd.read_csv(golden_dir / name)


def test_imbalance_matches_the_original_script(imbalance_frame):
    new = imbalance.add_sign(imbalance.latest_per_period(imbalance_frame))
    new = new.sort_values("settlement_period").reset_index(drop=True)

    old = golden("task1_imbalance_2025-12-07.csv")

    assert len(new) == len(old)
    assert list(new["settlement_date"]) == list(old["settlementDate"])
    assert list(new["settlement_period"]) == list(old["settlementPeriod"])
    assert new["value"].to_numpy() == pytest.approx(old["indicatedImbalance"].to_numpy())
    assert list(new["value_sign"]) == list(old["indicatedImbalance_sign"])


@pytest.mark.parametrize("series_name,name", [(schema.wind, "wind"), (schema.solar, "solar")])
def test_wind_solar_matches_the_original_script(forecast_frame, actual_frame, series_name, name):
    aligned = windsolar.align(forecast_frame, actual_frame)
    new = windsolar.split(aligned, series_name)
    new = new.sort_values("settlement_period").reset_index(drop=True)

    old = golden(f"task2_{name}_2025-11-11.csv")

    assert len(new) == len(old)
    assert list(new["settlement_period"]) == list(old["settlementPeriod"])

    for new_col, old_col in (("forecast_mw", "forecast_MW"),
                             ("actual_mw", "actual_MW"),
                             ("diff_mw", "diff_MW")):
        assert new[new_col].to_numpy() == pytest.approx(old[old_col].to_numpy())


@pytest.mark.parametrize("series_name,label,mean_error,mae", [
    (schema.wind, "Wind", 681.7, 1144.3),
    (schema.solar, "Solar", 8.1, 31.3),
])
def test_error_summary_matches_the_original_commentary(
    forecast_frame, actual_frame, series_name, label, mean_error, mae
):
    aligned = windsolar.align(forecast_frame, actual_frame)
    summary = windsolar.error_summary(windsolar.split(aligned, series_name), label)

    assert summary.mean_error == pytest.approx(mean_error, abs=0.05)
    assert summary.mean_absolute_error == pytest.approx(mae, abs=0.05)
