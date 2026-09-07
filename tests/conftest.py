"""Shared fixtures: a client wired to recorded payloads instead of the network."""

import pathlib
import sys

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1]))

from energyviz import schema
from energyviz.config import Settings
from energyviz.sources.elexon import ElexonClient

from support import FixtureHttpClient

imbalance_date = "2025-12-07"
windsolar_date = "2025-11-11"


@pytest.fixture
def settings():
    return Settings()


@pytest.fixture
def http():
    return FixtureHttpClient()


@pytest.fixture
def client(http, settings):
    return ElexonClient(http, settings)


@pytest.fixture
def imbalance_frame(client):
    return client.fetch_imbalance_local_day(imbalance_date)


@pytest.fixture
def forecast_frame(client):
    return client.fetch_wind_solar_local_day(windsolar_date, schema.generation_forecast)


@pytest.fixture
def actual_frame(client):
    return client.fetch_wind_solar_local_day(windsolar_date, schema.generation_actual)
