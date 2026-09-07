"""The Elexon source: raw BMRS payloads in, normalised frames out."""

import pytest

from energyviz import schema
from energyviz.errors import SchemaError
from energyviz.sources import elexon


def test_imbalance_local_day_is_a_valid_normalised_frame(imbalance_frame):
    schema.validate(imbalance_frame)

    assert set(imbalance_frame["dataset"]) == {schema.imbalance_forecast}
    assert set(imbalance_frame["source"]) == {"elexon"}
    assert set(imbalance_frame["zone"]) == {"GB"}
    assert set(imbalance_frame["unit"]) == {schema.mw}


def test_imbalance_keeps_all_four_indicated_quantities(imbalance_frame):
    assert set(imbalance_frame["series"]) == {
        schema.imbalance, schema.generation, schema.demand, schema.margin,
    }


def test_imbalance_covers_both_settlement_dates_of_the_local_day(imbalance_frame):
    assert set(imbalance_frame["settlement_date"]) == {"2025-12-06", "2025-12-07"}

    carried = imbalance_frame[imbalance_frame["settlement_date"] == "2025-12-06"]
    assert set(carried["settlement_period"]) == {47, 48}


def test_wind_solar_local_day_keeps_only_the_periods_it_asked_for(forecast_frame):
    schema.validate(forecast_frame)

    carried = forecast_frame[forecast_frame["settlement_date"] == "2025-12-06"]
    assert carried.empty

    early = forecast_frame[forecast_frame["settlement_date"] == "2025-11-10"]
    assert set(early["settlement_period"]) == {47, 48}


def test_actuals_surplus_day_is_discarded(actual_frame):
    """BMRS answers a one-day actuals query with 48 hours; the extra day goes."""
    assert set(actual_frame["settlement_date"]) == {"2025-11-10", "2025-11-11"}
    assert "2025-11-12" not in set(actual_frame["settlement_date"])


def test_wind_onshore_and_offshore_both_map_to_wind():
    assert elexon.map_psr_to_series("Wind Onshore") == schema.wind
    assert elexon.map_psr_to_series("Wind Offshore") == schema.wind
    assert elexon.map_psr_to_series("Solar") == schema.solar
    assert elexon.map_psr_to_series("Nuclear") is None
    assert elexon.map_psr_to_series(None) is None


def test_start_local_is_the_start_time_in_the_configured_zone(forecast_frame):
    row = forecast_frame.iloc[0]
    assert row["start_local"].tzinfo is not None
    assert row["start_local"].tz_convert("UTC") == row["start_utc"]


def test_a_local_day_costs_two_requests_with_a_pause_between(http, client):
    client.fetch_imbalance_local_day("2025-12-07")

    assert len(http.calls) == 2
    assert http.spacings == 1


def test_an_unknown_endpoint_names_the_ones_that_exist():
    with pytest.raises(KeyError, match="imbalance_evolution"):
        elexon.resolve_endpoint("nope")


def test_a_payload_without_the_expected_columns_raises(client, monkeypatch):
    monkeypatch.setattr(client.http, "get_json", lambda *a, **k: {"data": [{"x": 1}]})

    with pytest.raises(SchemaError):
        client.fetch_imbalance("2025-12-07", [1])
