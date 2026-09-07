"""
Poll an upstream feed until it publishes something new.

The loop knows nothing about imbalance, plots or BMRS. It is handed a way to
fetch a snapshot, a way to read the publish time off one, and something to do
when the publish time moves. The clock and the sleep are injected too, so the
whole thing can be exercised in a test without waiting half an hour.
"""

import datetime as dt
import logging
import time
from dataclasses import dataclass

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class WatchPolicy:
    """
    How often to look, and how hard to try when the feed is late.

    Parameters
    ----------
    interval_minutes : int
        Expected gap between upstream publications.
    retry : bool
        Whether to run the short retry sequence when the feed is late.
    retry_increments : tuple of int
        Seconds to wait before each retry, in order.
    """

    interval_minutes: int = 30
    retry: bool = True
    retry_increments: tuple = (30, 60, 120)


def countdown(seconds):
    """
    Tick down on one terminal line, so a long wait does not look like a hang.
    """
    while seconds:
        minutes, secs = divmod(seconds, 60)
        hours, minutes = divmod(minutes, 60)
        print(f" Next update in: {hours:02d}:{minutes:02d}:{secs:02d}", end="\r", flush=True)
        time.sleep(1)
        seconds -= 1
    print(" Checking for new data..." + " " * 30)


def silent(seconds):
    """A `wait` that returns at once. For tests."""
    return None


def poll(fetch, published_at, on_update, policy=WatchPolicy(), wait=countdown, now=None):
    """
    Watch a feed forever, calling `on_update` each time it moves.

    Parameters
    ----------
    fetch : callable
        Takes no arguments, returns a snapshot.
    published_at : callable
        Takes a snapshot, returns its publish timestamp (tz-aware).
    on_update : callable
        Called as `on_update(previous, latest, cycle)` when a newer snapshot
        arrives. Its return value is ignored.
    policy : WatchPolicy
    wait : callable
        Takes a number of seconds and blocks for them.
    now : callable, optional
        Takes a tzinfo and returns the current time in it. Injected for tests;
        defaults to the system clock.

    Notes
    -----
    Runs until interrupted. The caller owns the first snapshot: `poll` fetches
    it, so anything that should happen to it happens through `on_update` being
    called with `previous=None` on cycle 0.
    """
    now = now if now is not None else (lambda tz: dt.datetime.now(tz=tz))

    previous = fetch()
    previous_publish = published_at(previous)
    on_update(None, previous, 0)
    log.info(f" Initial latest publish time: {previous_publish}")

    cycle = 1

    while True:
        _wait_for_next(previous_publish, policy, cycle, wait, now)

        log.info(f" Update cycle {cycle}: Checking for new data...")
        latest = fetch()
        latest_publish = published_at(latest)

        log.info(f"Previous publish: {previous_publish}")
        log.info(f"New publish:      {latest_publish}")
        log.info(f"Has new data:     {latest_publish > previous_publish}")

        if latest_publish > previous_publish:
            log.info(" New data found on first attempt!")
            on_update(previous, latest, cycle)
            previous, previous_publish = latest, latest_publish
            cycle += 1
            continue

        if not policy.retry:
            log.info(" No new data, and retry disabled. Loop continues to next interval.")
            cycle += 1
            continue

        found = _retry_sequence(
            fetch, published_at, on_update, policy, wait, previous, previous_publish, cycle
        )

        if found is not None:
            previous, previous_publish = found
        else:
            log.info(" No new data after all retries. Waiting until next expected interval.")

        cycle += 1


def _wait_for_next(previous_publish, policy, cycle, wait, now):
    """Sleep until the next publication is due, or return at once if it is overdue."""
    expected = previous_publish + dt.timedelta(minutes=policy.interval_minutes)
    seconds = (expected - now(expected.tzinfo)).total_seconds()

    if seconds > 0:
        log.info(
            f"\n Update cycle {cycle}: waiting {seconds / 60:.1f} minutes "
            f"until next expected update at {expected}..."
        )
        wait(int(seconds))
    else:
        log.info(
            f"\n Update cycle {cycle}: expected update time {expected} is already "
            f"{abs(seconds) / 60:.1f} minutes in the past, checking now..."
        )


def _retry_sequence(fetch, published_at, on_update, policy, wait,
                    previous, previous_publish, cycle):
    """
    Work through the retry increments, stopping at the first newer snapshot.

    Returns the (snapshot, publish time) that won, or None if none did.
    """
    log.info(" No new data on first attempt. Starting retry sequence...")

    for increment in policy.retry_increments:
        log.info(f" Retrying in {increment} seconds...")
        wait(increment)

        latest = fetch()
        latest_publish = published_at(latest)
        log.info(f" Retry check — new publish: {latest_publish}")

        if latest_publish > previous_publish:
            log.info(" New data found after retry!")
            on_update(previous, latest, cycle)
            return latest, latest_publish

    return None
