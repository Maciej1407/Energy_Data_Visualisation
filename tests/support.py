"""
Test helpers: serve recorded BMRS payloads instead of talking to the network.
"""

import json
import pathlib

fixture_dir = pathlib.Path(__file__).parent / "fixtures"


def load(name):
    """Read a recorded payload by fixture name."""
    return json.loads((fixture_dir / f"{name}.json").read_text())


class FixtureHttpClient:
    """
    Stands in for energyviz.transport.HttpClient.

    Resolves a request to a recorded payload by endpoint path and the date that
    identifies it, and records every call it served so tests can assert on the
    request pattern.
    """

    def __init__(self):
        self.calls = []
        self.spacings = 0

    def get_json(self, url, params, label="request"):
        self.calls.append((url, dict(params), label))

        if "evolution" in url:
            date = params["settlementDate"]
            periods = params["settlementPeriod"]
            tag = "sp47-48" if 47 in periods else "sp1-46"
            return load(f"imbalance_evolution_{date}_{tag}")

        date = params["from"][:10]
        if "forecast" in url:
            return load(f"windsolar_forecast_{date}")
        return load(f"windsolar_actuals_{date}")

    def space(self):
        self.spacings += 1
