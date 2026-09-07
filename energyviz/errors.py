"""
Typed errors raised at package boundaries.

Library code raises; only the CLI decides what to do about it. Nothing in
energyviz prints an error and carries on.
"""


class EnergyVizError(Exception):
    """Base class for every error this package raises."""


class FetchError(EnergyVizError):
    """A remote API could not be reached, or refused to answer."""


class SchemaError(EnergyVizError):
    """A payload or frame did not match the shape we expect."""
