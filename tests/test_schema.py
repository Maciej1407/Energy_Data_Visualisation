"""The contract every frame in the package has to satisfy."""

import pandas as pd
import pytest

from energyviz import schema
from energyviz.errors import SchemaError


def test_empty_frame_carries_the_whole_contract():
    df = schema.empty_frame()
    assert list(df.columns) == schema.columns
    assert df.empty


def test_conform_fills_missing_optional_columns():
    df = schema.conform(pd.DataFrame({
        "source": ["elexon"],
        "dataset": [schema.imbalance_forecast],
        "series": [schema.imbalance],
        "value": [1.0],
    }))

    assert list(df.columns) == schema.columns
    assert pd.isna(df["publish_utc"].iloc[0])


def test_validate_rejects_a_series_outside_the_vocabulary():
    df = schema.conform(pd.DataFrame({
        "dataset": [schema.imbalance_forecast],
        "series": ["nuclear"],
        "value": [1.0],
    }))

    with pytest.raises(SchemaError, match="nuclear"):
        schema.validate(df)


def test_validate_rejects_a_frame_missing_contract_columns():
    with pytest.raises(SchemaError, match="missing contract columns"):
        schema.validate(pd.DataFrame({"value": [1.0]}))


def test_select_narrows_by_dataset_and_series(imbalance_frame):
    only = schema.select(
        imbalance_frame,
        dataset=schema.imbalance_forecast,
        series_name=schema.imbalance,
    )

    assert set(only["series"]) == {schema.imbalance}
    assert len(only) < len(imbalance_frame)
