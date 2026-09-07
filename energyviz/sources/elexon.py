"""
Elexon BMRS (GB) source.

Fetches from the BMRS Insights REST API and hands back frames that satisfy
energyviz.schema. Nothing downstream of this module knows that BMRS calls a
value `quantity`, files wind under `psrType`, or splits a local day across two
settlement dates.

Adding a second source means writing the equivalent of this one file: three
fetch methods that end in `schema.conform`, and an entry in sources/__init__.py.
"""

import logging
from dataclasses import dataclass

import pandas as pd

from ..domain import settlement
from ..errors import SchemaError
from .. import schema

log = logging.getLogger(__name__)

source_name = "elexon"
zone = "GB"


# =========================
# Endpoint table
# =========================

@dataclass(frozen=True)
class Endpoint:
    """One BMRS endpoint and the dataset it yields."""

    path: str
    dataset: str
    unit: str = schema.mw


endpoints = {
    "imbalance_evolution": Endpoint(
        "/forecast/indicated/day-ahead/evolution",
        schema.imbalance_forecast,
    ),
    "wind_solar_forecast": Endpoint(
        "/forecast/generation/wind-and-solar/day-ahead",
        schema.generation_forecast,
    ),
    "wind_solar_actuals": Endpoint(
        "/generation/actual/per-type/wind-and-solar",
        schema.generation_actual,
    ),
}


def resolve_endpoint(name):
    """
    Look up an endpoint by name.

    Raises
    ------
    KeyError
        If the name is not one we know about.
    """
    endpoint = endpoints.get(name)
    if endpoint is None:
        raise KeyError(f"Unknown endpoint '{name}'; known: {sorted(endpoints)}")
    return endpoint


# The four indicated quantities the imbalance feed reports, and the series each
# becomes. Kept as a table so that adding one is a one-line change.
indicated_columns = {
    "indicatedImbalance": schema.imbalance,
    "indicatedGeneration": schema.generation,
    "indicatedDemand": schema.demand,
    "indicatedMargin": schema.margin,
}


def map_psr_to_series(psr):
    """
    Map a BMRS `psrType` to a schema series.

    Matched on substring so that 'Wind Onshore' and 'Wind Offshore' both land on
    wind, and anything unrecognised comes back as None to be filtered out.
    """
    if psr is None:
        return None

    psr_lower = str(psr).lower()

    if "solar" in psr_lower:
        return schema.solar
    if "wind" in psr_lower:
        return schema.wind

    return None


# =========================
# Client
# =========================

class ElexonClient:
    """
    Fetch BMRS data as normalised frames.

    Parameters
    ----------
    http : energyviz.transport.HttpClient
        Does the retrying and the JSON decoding.
    settings : energyviz.config.Settings
        Supplies the base URL and the local timezone.
    """

    def __init__(self, http, settings):
        self.http = http
        self.settings = settings

    # -- low level --------------------------------------------------------

    def _get(self, endpoint_name, params, label):
        endpoint = resolve_endpoint(endpoint_name)
        url = self.settings.base_url + endpoint.path
        payload = self.http.get_json(url, params, label=label)

        if "data" not in payload:
            raise SchemaError(
                f"{label}: response has no 'data' key; got: {list(payload)}"
            )

        return endpoint, pd.DataFrame(payload["data"])

    # -- imbalance --------------------------------------------------------

    def fetch_imbalance(self, settlement_date, periods):
        """
        Fetch the indicated day-ahead imbalance evolution for one settlement date.

        Parameters
        ----------
        settlement_date : str
            UTC settlement date, 'YYYY-MM-DD'.
        periods : list of int
            Settlement periods to ask for.
        """
        params = {
            "settlementDate": settlement_date,
            "settlementPeriod": list(periods),
            "format": "json",
        }
        label = f"Imbalance {settlement_date} SP{periods[0]}-{periods[-1]}"
        endpoint, raw = self._get("imbalance_evolution", params, label)
        return self._normalise_imbalance(raw, endpoint)

    def fetch_imbalance_local_day(self, date):
        """
        Fetch a whole local day of imbalance evolution.

        Issues one request per window returned by
        `settlement.local_day_windows`, spaced by the retry policy, and returns
        the concatenated normalised frame.
        """
        frames = []
        for index, (day, periods) in enumerate(settlement.local_day_windows(date)):
            if index > 0:
                self.http.space()
            frames.append(self.fetch_imbalance(day, periods))

        return schema.conform(pd.concat(frames, ignore_index=True))

    def _normalise_imbalance(self, raw, endpoint):
        if raw.empty:
            return schema.empty_frame()

        present = [col for col in indicated_columns if col in raw.columns]
        if not present:
            raise SchemaError(
                f"Imbalance payload has none of {list(indicated_columns)}; "
                f"got: {list(raw.columns)}"
            )

        long = raw.melt(
            id_vars=["settlementDate", "settlementPeriod", "startTime", "publishTime"],
            value_vars=present,
            var_name="indicated_column",
            value_name="value",
        )
        long["series"] = long["indicated_column"].map(indicated_columns)

        return self._finish(long, endpoint)

    # -- wind and solar ---------------------------------------------------

    def fetch_wind_solar_forecast(self, settlement_date):
        """
        Fetch the day-ahead wind and solar generation forecast for one UTC day.
        """
        params = {
            "from": f"{settlement_date}T00:00Z",
            "to": f"{settlement_date}T23:30Z",
            "processType": "Day ahead",
            "format": "json",
        }
        endpoint, raw = self._get(
            "wind_solar_forecast", params, f"Forecast {settlement_date}"
        )
        return self._normalise_wind_solar(raw, endpoint)

    def fetch_wind_solar_actuals(self, settlement_date):
        """
        Fetch actual / estimated wind and solar generation for one UTC day.

        Note that BMRS answers this one with a 48-hour window: asking for a
        single day also returns the following day's periods. Downstream joins
        are on (settlement date, period), so the surplus rows fall away.
        """
        next_day = settlement.next_date(settlement_date)

        params = {
            "from": f"{settlement_date}T00:00Z",
            "to": f"{next_day}T00:00Z",
            "settlementPeriodFrom": 1,
            "settlementPeriodTo": 48,
            "format": "json",
        }
        endpoint, raw = self._get(
            "wind_solar_actuals", params, f"Actuals {settlement_date}"
        )
        return self._normalise_wind_solar(raw, endpoint)

    def fetch_wind_solar_local_day(self, date, dataset):
        """
        Fetch a local day of wind and solar, forecast or actual.

        Parameters
        ----------
        date : str
            UTC settlement date the local day is anchored on.
        dataset : str
            `schema.generation_forecast` or `schema.generation_actual`.
        """
        fetchers = {
            schema.generation_forecast: self.fetch_wind_solar_forecast,
            schema.generation_actual: self.fetch_wind_solar_actuals,
        }

        fetch = fetchers.get(dataset)
        if fetch is None:
            raise KeyError(
                f"Unknown wind/solar dataset '{dataset}'; known: {sorted(fetchers)}"
            )

        frames = []
        for index, (day, periods) in enumerate(settlement.local_day_windows(date)):
            if index > 0:
                self.http.space()
            day_frame = fetch(day)
            frames.append(
                day_frame[
                    (day_frame["settlement_date"] == day)
                    & (day_frame["settlement_period"].isin(periods))
                ]
            )

        return schema.conform(pd.concat(frames, ignore_index=True))

    def _normalise_wind_solar(self, raw, endpoint):
        if raw.empty:
            return schema.empty_frame()

        for col in ("psrType", "quantity"):
            if col not in raw.columns:
                raise SchemaError(
                    f"Wind/solar payload missing '{col}'; got: {list(raw.columns)}"
                )

        long = raw.copy()
        long["series"] = long["psrType"].apply(map_psr_to_series)
        long["value"] = long["quantity"]
        long = long[long["series"].notna()]

        return self._finish(long, endpoint)

    # -- shared tail ------------------------------------------------------

    def _finish(self, long, endpoint):
        """
        Attach the identifying columns and conform to the schema.

        Every normaliser ends here, so the contract is applied in exactly one
        place per source.
        """
        out = pd.DataFrame({
            "source": source_name,
            "dataset": endpoint.dataset,
            "zone": zone,
            "settlement_date": long["settlementDate"].astype(str),
            "settlement_period": long["settlementPeriod"],
            "start_utc": pd.to_datetime(long["startTime"], utc=True),
            "publish_utc": pd.to_datetime(long.get("publishTime"), utc=True),
            "series": long["series"],
            "value": pd.to_numeric(long["value"], errors="coerce"),
            "unit": endpoint.unit,
        })

        out["start_local"] = settlement.to_local(out["start_utc"], self.settings.timezone)

        return schema.conform(out)
