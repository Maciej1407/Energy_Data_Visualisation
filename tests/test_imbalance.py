"""Reducing an evolving forecast, labelling it, and diffing two snapshots."""

import pandas as pd

from energyviz import schema
from energyviz.domain import imbalance


def test_latest_per_period_keeps_one_row_per_period(imbalance_frame):
    latest = imbalance.latest_per_period(imbalance_frame)

    assert len(latest) == 48
    assert not latest.duplicated(["settlement_date", "settlement_period"]).any()
    assert set(latest["series"]) == {schema.imbalance}


def test_latest_per_period_keeps_the_most_recently_published_row(imbalance_frame):
    latest = imbalance.latest_per_period(imbalance_frame)

    one = schema.select(
        imbalance_frame, schema.imbalance_forecast, schema.imbalance,
    )
    one = one[(one["settlement_date"] == "2025-12-07") & (one["settlement_period"] == 10)]
    expected = one.sort_values("publish_utc").iloc[-1]

    got = latest[latest["settlement_period"] == 10].iloc[0]
    assert got["value"] == expected["value"]
    assert got["publish_utc"] == expected["publish_utc"]


def test_rows_with_no_value_are_dropped(imbalance_frame):
    frame = imbalance_frame.copy()
    frame.loc[frame["settlement_period"] == 10, "value"] = pd.NA

    latest = imbalance.latest_per_period(frame)
    assert 10 not in set(latest["settlement_period"])


def test_sign_splits_at_zero():
    df = pd.DataFrame({"value": [5.0, -5.0, 0.0, None]})
    signed = imbalance.add_sign(df)

    assert list(signed["value_sign"]) == ["Positive", "Negative", "Positive", None]


def test_diff_measures_the_revision(imbalance_frame):
    latest = imbalance.add_sign(imbalance.latest_per_period(imbalance_frame))

    previous = latest.copy()
    previous["value"] = previous["value"] - 100.0

    merged, _ = imbalance.diff_snapshots(previous, latest)

    assert len(merged) == 48
    assert (merged["delta"].round(6) == 100.0).all()


def test_diff_across_different_dates_matches_on_period_alone(imbalance_frame):
    latest = imbalance.latest_per_period(imbalance_frame)

    previous = latest.copy()
    previous["settlement_date"] = "2025-01-01"

    merged, same_date = imbalance.diff_snapshots(previous, latest)

    assert same_date is False
    assert len(merged) == 48


def test_latest_publish_is_none_when_nothing_was_published():
    df = pd.DataFrame({"publish_utc": pd.Series([None, None], dtype="datetime64[ns, UTC]")})
    assert imbalance.latest_publish(df) is None
