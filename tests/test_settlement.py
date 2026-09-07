"""Local-day arithmetic: the 47, 48, 1..46 rule."""

from energyviz.domain import settlement


def test_local_day_takes_47_and_48_from_the_previous_date():
    windows = settlement.local_day_windows("2025-12-07")

    assert windows[0] == ("2025-12-06", [47, 48])
    assert windows[1][0] == "2025-12-07"
    assert windows[1][1] == list(range(1, 47))


def test_previous_date_crosses_a_year_boundary():
    assert settlement.previous_date("2025-01-01") == "2024-12-31"


def test_period_order_puts_the_carried_over_periods_first():
    order = settlement.period_order()

    assert order[:2] == ["47", "48"]
    assert order[2] == "1"
    assert order[-1] == "46"
    assert len(order) == 48


def test_add_period_labels_sorts_into_local_day_order(imbalance_frame):
    labelled = settlement.add_period_labels(imbalance_frame)

    assert labelled["settlement_period_str"].iloc[0] == "47"
    assert labelled["period_position"].is_monotonic_increasing
