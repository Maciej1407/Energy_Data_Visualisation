"""Figure builders: a Figure is a value, so it can be inspected without a browser."""

import pytest

from energyviz import schema
from energyviz.domain import imbalance, windsolar
from energyviz.viz import figures, theme


@pytest.fixture
def snapshot(imbalance_frame):
    return imbalance.add_sign(imbalance.latest_per_period(imbalance_frame))


def test_snapshot_title_names_the_day_and_the_publish_time(snapshot, settings):
    title = figures.snapshot_title(snapshot, settings.timezone)

    assert title.startswith("Indicated Imbalance per Settlement Period — 07 Dec 2025,")
    assert "CET" in title


def test_snapshot_splits_into_a_positive_and_a_negative_trace(snapshot, settings):
    fig = figures.imbalance_snapshot(snapshot, settings.timezone)

    names = sorted(trace.name for trace in fig.data)
    assert names == ["Negative", "Positive"]
    assert fig.layout.paper_bgcolor == theme.ft.paper_bg


def test_diff_draws_a_stem_for_every_revised_period(snapshot):
    previous = snapshot.copy()
    previous["value"] = previous["value"] - 100.0

    merged, _ = imbalance.diff_snapshots(previous, snapshot)
    fig = figures.imbalance_diff(merged)

    stems = [trace for trace in fig.data if trace.mode == "lines"]
    assert len(stems) == 48


def test_diff_title_names_both_publish_times(snapshot, settings):
    previous = snapshot.copy()
    merged_title = figures.diff_title(previous, snapshot, False, settings.timezone, suffix="Update 3")

    assert "vs" in merged_title
    assert merged_title.endswith("(Update 3)")


def test_forecast_vs_actual_carries_two_lines_and_a_table(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)
    fig = figures.forecast_vs_actual(windsolar.split(aligned, schema.wind), "Wind")

    assert [trace.name for trace in fig.data[:2]] == ["Wind forecast", "Wind actual"]
    assert fig.data[2].type == "table"
    assert len(fig.data[2].cells.values[0]) == 48


def test_a_second_theme_needs_no_edit_to_the_library(snapshot, settings):
    midnight = theme.Theme(paper_bg="#101418", green="#4ade80", red="#f87171")
    fig = figures.imbalance_snapshot(snapshot, settings.timezone, theme=midnight)

    assert fig.layout.paper_bgcolor == "#101418"


def test_an_empty_frame_is_refused_rather_than_drawn_blank(forecast_frame, actual_frame):
    aligned = windsolar.align(forecast_frame, actual_frame)
    empty = aligned.iloc[0:0]

    with pytest.raises(ValueError, match="no data to plot"):
        figures.forecast_vs_actual(empty, "Wind")
