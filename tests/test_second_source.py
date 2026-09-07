"""
Proof that a second source drops into the seam.

The point of the normalised schema is that adding a source should cost one
module and one registry line, and change nothing downstream. This test makes
that claim checkable rather than aspirational: it implements an ENTSOE day-ahead
price source here, in the test file, against a real A44 payload - XML rather
than JSON, hourly rather than half-hourly, priced in EUR/MWh rather than
measured in MW, and with no concept of a settlement period - and then runs it
through the same schema, the same store, and the same frame operations as
Elexon.

If a change to energyviz makes a second source harder to add, this fails.
"""

import datetime as dt
import pathlib
import xml.etree.ElementTree as ET

import pandas as pd
import pytest

from energyviz import schema, sources
from energyviz.config import Settings
from energyviz.domain import settlement
from energyviz.store import parquet

fixture = pathlib.Path(__file__).parent / "fixtures" / "entsoe_day_ahead_PL_2026-08-04.xml"
namespace = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:0"}

source_name = "entsoe"

# The same shape as elexon.domainCodes - a table, resolved by a function.
domain_codes = {
    "PL": "10YPL-AREA-----S",
}

resolutions = {
    "PT60M": dt.timedelta(hours=1),
    "PT30M": dt.timedelta(minutes=30),
    "PT15M": dt.timedelta(minutes=15),
}


def resolve_domain(zone):
    code = domain_codes.get(zone)
    if code is None:
        raise KeyError(f"Unknown zone '{zone}'; known: {sorted(domain_codes)}")
    return code


class EntsoeClient:
    """A minimal ENTSOE day-ahead price source, written to the same contract."""

    def __init__(self, http, settings):
        self.http = http
        self.settings = settings

    def fetch_day_ahead_prices(self, zone, start, end):
        resolve_domain(zone)
        body = self.http.get_text(
            self.settings.base_url,
            {"documentType": "A44", "periodStart": start, "periodEnd": end},
        )
        return self._normalise(body, zone)

    def _normalise(self, body, zone):
        root = ET.fromstring(body)
        rows = []

        for period in root.iterfind(".//ns:Period", namespace):
            start = pd.Timestamp(period.findtext("ns:timeInterval/ns:start", namespaces=namespace))
            step = resolutions[period.findtext("ns:resolution", namespaces=namespace)]

            for point in period.iterfind("ns:Point", namespace):
                position = int(point.findtext("ns:position", namespaces=namespace))
                rows.append({
                    "start_utc": start + (position - 1) * step,
                    "value": float(point.findtext("ns:price.amount", namespaces=namespace)),
                })

        frame = pd.DataFrame(rows)
        frame["source"] = source_name
        frame["dataset"] = schema.day_ahead_price
        frame["zone"] = zone
        frame["series"] = schema.price
        frame["unit"] = schema.eur_mwh
        frame["start_utc"] = pd.to_datetime(frame["start_utc"], utc=True)
        frame["start_local"] = settlement.to_local(frame["start_utc"], self.settings.timezone)
        frame["settlement_date"] = frame["start_local"].dt.strftime(settlement.date_format)
        # Hourly, so there is no settlement period. That is what the nullable
        # column in the schema is for.
        frame["settlement_period"] = pd.NA

        return schema.conform(frame)


class XmlHttpClient:
    """Serves the recorded XML. Stands in for transport.HttpClient."""

    def get_text(self, url, params):
        return fixture.read_text()


@pytest.fixture
def entsoe():
    return EntsoeClient(XmlHttpClient(), Settings(base_url="https://web-api.tp.entsoe.eu/api"))


@pytest.fixture
def prices(entsoe):
    return entsoe.fetch_day_ahead_prices("PL", "202608042200", "202608052200")


# =========================
# The seam
# =========================

def test_a_foreign_payload_reaches_the_schema(prices):
    schema.validate(prices)

    assert list(prices.columns) == schema.columns
    assert len(prices) == 6
    assert set(prices["source"]) == {"entsoe"}
    assert set(prices["zone"]) == {"PL"}
    assert set(prices["unit"]) == {schema.eur_mwh}


def test_hourly_rows_carry_no_settlement_period(prices):
    assert prices["settlement_period"].isna().all()
    assert prices["settlement_period"].dtype == "Int64"


def test_positions_become_real_timestamps(prices):
    assert prices["start_utc"].iloc[0] == pd.Timestamp("2026-08-04T22:00Z")
    assert prices["start_utc"].iloc[5] == pd.Timestamp("2026-08-05T03:00Z")
    assert prices["value"].iloc[0] == pytest.approx(84.21)


def test_it_registers_like_any_other_source(monkeypatch):
    monkeypatch.setitem(sources.builders, source_name, EntsoeClient)

    client = sources.build(source_name, Settings())
    assert isinstance(client, EntsoeClient)


def test_two_sources_live_in_one_frame(prices, forecast_frame):
    both = pd.concat([prices, forecast_frame], ignore_index=True)
    schema.validate(schema.conform(both))

    assert set(both["source"]) == {"entsoe", "elexon"}
    assert set(both["unit"]) == {schema.eur_mwh, schema.mw}

    # The GB half-hourly rows keep their periods; the PL hourly rows do not.
    assert both[both["source"] == "elexon"]["settlement_period"].notna().all()
    assert both[both["source"] == "entsoe"]["settlement_period"].isna().all()


def test_two_sources_share_one_store(prices, forecast_frame, tmp_path):
    parquet.write(pd.concat([prices, forecast_frame], ignore_index=True), tmp_path)

    written = sorted(str(p.relative_to(tmp_path)) for p in tmp_path.rglob("*.parquet"))
    assert "day_ahead_price/PL/2026-08-05.parquet" in written
    assert "generation_forecast/GB/2025-11-11.parquet" in written

    back = parquet.read(tmp_path, dataset=schema.day_ahead_price)
    assert back["value"].tolist() == prices["value"].tolist()


def test_select_works_across_sources_unchanged(prices, forecast_frame):
    both = schema.conform(pd.concat([prices, forecast_frame], ignore_index=True))

    assert len(schema.select(both, dataset=schema.day_ahead_price)) == 6
    assert set(schema.select(both, series_name=schema.price)["source"]) == {"entsoe"}


def test_an_unknown_zone_names_the_ones_that_exist():
    with pytest.raises(KeyError, match="PL"):
        resolve_domain("XX")
