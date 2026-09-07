"""
Task 2 - wind and solar day-ahead forecast against actual outturn.

Fetches a local day of both, aligns them per settlement period, plots each fuel
over a table of the same numbers, and prints the forecast error summary.
"""

import argparse
import logging
import sys

import pandas as pd

from energyviz import schema, sources
from energyviz.domain import windsolar
from energyviz.errors import EnergyVizError
from energyviz.store import parquet
from energyviz.viz import export, figures

from . import runtime

log = logging.getLogger("energyviz.cli.windsolar")

# The old task2.py spelled these after the DataFrame columns; both are accepted
# so that anything already scripted against the flag keeps working.
x_axis_aliases = {
    "settlementPeriod": "settlement_period",
    "startTime_cest": "start_local",
    "settlement_period": "settlement_period",
    "start_local": "start_local",
}

fuels = [
    (schema.wind, "Wind"),
    (schema.solar, "Solar"),
]


def output_name(df, label):
    """Filename stem, dated from the local start time as task2.py always did."""
    date_str = df["start_local"].iloc[0].strftime("%d %b %Y").replace(" ", "_")
    return f"forecast_vs_actual_{label.lower()}_{date_str}"


def run(args):
    """Wire everything together and run."""
    settings = runtime.settings_from(args)
    client = sources.build("elexon", settings)
    x_axis = x_axis_aliases[args.x_axis]

    log.info(f"Part 2 - wind & solar forecast vs actuals for local day {args.date}")

    forecast = client.fetch_wind_solar_local_day(args.date, schema.generation_forecast)
    actual = client.fetch_wind_solar_local_day(args.date, schema.generation_actual)

    log.info(f"Forecast rows (local day): {len(forecast)}")
    log.info(f"Actual rows   (local day): {len(actual)}")

    if args.parquet_dir is not None:
        parquet.write(pd.concat([forecast, actual], ignore_index=True), args.parquet_dir)

    aligned = windsolar.align(forecast, actual)

    results = {}
    for series_name, label in fuels:
        df = windsolar.split(aligned, series_name)
        results[series_name] = df
        log.info(f"{label} rows (merged): {len(df)}")

        if args.do_plots and not df.empty:
            plot(df, label, args, x_axis)

    for series_name, label in fuels:
        summary = windsolar.error_summary(results[series_name], label)
        if summary is None:
            log.info(f"{label}: no data.")
            continue
        for line in summary.lines():
            log.info(line)

    return results


def plot(df, label, args, x_axis):
    """Draw, save and optionally show one fuel."""
    fig = figures.forecast_vs_actual(df, label, x_axis=x_axis)
    export.save(
        fig, args.output_dir, output_name(df, label),
        height=export.height_for_table(len(df)),
    )
    if args.show:
        fig.show()


# =========================
# CLI
# =========================

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="BMRS wind & solar forecast vs actuals (Part 2).",
    )
    runtime.add_common_arguments(parser)

    parser.add_argument(
        "--x-axis",
        choices=sorted(x_axis_aliases),
        default="settlement_period",
        help="X-axis variable for plots (default: settlement_period).",
    )
    parser.add_argument(
        "--no-plots",
        dest="do_plots",
        action="store_false",
        help="Disable plotting (just fetch & compute errors).",
    )
    parser.set_defaults(do_plots=True)

    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    runtime.configure_logging(args.verbose)

    try:
        run(args)
    except KeyboardInterrupt:
        log.info("\nStopped.")
    except EnergyVizError as e:
        log.error(f"{type(e).__name__}: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
