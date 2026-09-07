"""Parquet round-trip: what goes in is what comes back."""

import pandas as pd
import pytest

from energyviz import schema
from energyviz.errors import SchemaError
from energyviz.store import parquet


def test_write_splits_by_dataset_zone_and_date(forecast_frame, actual_frame, tmp_path):
    both = pd.concat([forecast_frame, actual_frame], ignore_index=True)
    paths = parquet.write(both, tmp_path)

    written = sorted(str(p.relative_to(tmp_path)) for p in paths)
    assert written == [
        "generation_actual/GB/2025-11-10.parquet",
        "generation_actual/GB/2025-11-11.parquet",
        "generation_forecast/GB/2025-11-10.parquet",
        "generation_forecast/GB/2025-11-11.parquet",
    ]


def test_round_trip_preserves_every_value(forecast_frame, tmp_path):
    parquet.write(forecast_frame, tmp_path)
    back = parquet.read(tmp_path)

    order = ["settlement_date", "settlement_period", "series", "value"]
    assert (
        back.sort_values(order).reset_index(drop=True)
        .equals(forecast_frame.sort_values(order).reset_index(drop=True))
    )


def test_read_narrows_by_dataset_and_date(forecast_frame, actual_frame, tmp_path):
    parquet.write(pd.concat([forecast_frame, actual_frame], ignore_index=True), tmp_path)

    one = parquet.read(tmp_path, dataset=schema.generation_actual, settlement_date="2025-11-11")

    assert set(one["dataset"]) == {schema.generation_actual}
    assert set(one["settlement_date"]) == {"2025-11-11"}


def test_read_of_nothing_says_so(tmp_path):
    with pytest.raises(FileNotFoundError):
        parquet.read(tmp_path)


def test_an_invalid_frame_is_never_written(tmp_path):
    bad = schema.conform(pd.DataFrame({"dataset": ["prices"], "value": [1.0]}))

    with pytest.raises(SchemaError):
        parquet.write(bad, tmp_path)

    assert list(tmp_path.iterdir()) == []
