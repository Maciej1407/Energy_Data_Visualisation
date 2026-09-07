"""
Task 1 - indicated day-ahead imbalance, and how the forecast evolves.

Fetches a local day, plots the live forecast, then watches the feed and plots
the revision each time BMRS republishes.
"""

import argparse
import logging
import sys

from energyviz import sources, watch
from energyviz.domain import imbalance
from energyviz.errors import EnergyVizError
from energyviz.store import parquet
from energyviz.viz import export, figures

from . import runtime

log = logging.getLogger("energyviz.cli.imbalance")


# =========================
# Output naming
# =========================

def snapshot_name(date):
    """Filename stem for a single snapshot, named for the day requested."""
    return f"part1_imbalance_{date}"


def diff_name(date, published):
    """Filename stem for a revision plot, stamped with the new publish time."""
    return f"part1_diff_{date}_{published.strftime('%Y%m%dT%H%M%S')}"


# =========================
# Run
# =========================

def snapshot(client, parquet_dir=None):
    """
    Fetch a local day and reduce it to the live forecast, one row per period.

    Optionally archives the full evolution frame as Parquet on the way past.
    """
    def fetch(date):
        raw = client.fetch_imbalance_local_day(date)
        if parquet_dir is not None:
            parquet.write(raw, parquet_dir)
        return imbalance.add_sign(imbalance.latest_per_period(raw))

    return fetch


def plot_snapshot(df, args, settings):
    """Draw, save and optionally show the live forecast."""
    fig = figures.imbalance_snapshot(df, settings.timezone)
    export.save(fig, args.output_dir, snapshot_name(args.date))
    if args.show:
        fig.show()


def plot_revision(previous, latest, cycle, args, settings):
    """Draw, save and optionally show how the forecast moved."""
    merged, same_date = imbalance.diff_snapshots(previous, latest)
    title = figures.diff_title(
        previous, latest, same_date, settings.timezone, suffix=f"Update {cycle}"
    )

    fig = figures.imbalance_diff(merged, title=title)
    export.save(
        fig, args.output_dir,
        diff_name(args.date, imbalance.latest_publish(latest)),
    )
    if args.show:
        fig.show()


def run(args):
    """Wire everything together and run, once or forever."""
    settings = runtime.settings_from(args)
    client = sources.build("elexon", settings)
    fetch = snapshot(client, parquet_dir=args.parquet_dir)

    if not args.auto_update:
        plot_snapshot(fetch(args.date), args, settings)
        return

    log.info(f" Starting auto-update loop for settlement date: {args.date}")

    def on_update(previous, latest, cycle):
        if previous is None:
            plot_snapshot(latest, args, settings)
        else:
            plot_revision(previous, latest, cycle, args, settings)

    watch.poll(
        fetch=lambda: fetch(args.date),
        published_at=imbalance.latest_publish,
        on_update=on_update,
        policy=watch.WatchPolicy(
            interval_minutes=args.update_interval_minutes,
            retry=args.retry,
            retry_increments=tuple(args.retry_increments),
        ),
    )


# =========================
# CLI
# =========================

def parse_args(argv=None):
    parser = argparse.ArgumentParser(
        description="BMRS indicated imbalance auto-update visualiser",
    )
    runtime.add_common_arguments(parser)

    parser.add_argument(
        "--update-interval-minutes",
        type=int,
        default=30,
        help="Minutes between expected forecast updates (default: 30).",
    )
    parser.add_argument(
        "--retry-increments",
        type=int,
        nargs="+",
        default=[30, 60, 120],
        help="Retry delays in seconds if no new data is found on first attempt "
             "(default: 30 60 120).",
    )
    parser.add_argument(
        "--no-retry",
        dest="retry",
        action="store_false",
        help="Disable short retry sequence between main intervals.",
    )
    parser.add_argument(
        "--no-auto-update",
        dest="auto_update",
        action="store_false",
        help="Plot the latest data once and exit, instead of watching for updates.",
    )
    parser.set_defaults(retry=True, auto_update=True)

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
