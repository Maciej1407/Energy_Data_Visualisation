"""The poll loop, driven by an injected clock so it runs instantly."""

import datetime as dt

import pytest

from energyviz import watch

tz = dt.timezone.utc
start = dt.datetime(2026, 1, 1, 12, 0, tzinfo=tz)


class Feed:
    """A feed that publishes on a script, and stops the loop when it is done."""

    def __init__(self, publishes, stop_after):
        self.publishes = list(publishes)
        self.stop_after = stop_after
        self.updates = []

    def fetch(self):
        return {"publish": self.publishes[0] if len(self.publishes) == 1
                else self.publishes.pop(0)}

    def on_update(self, previous, latest, cycle):
        self.updates.append((cycle, previous, latest["publish"]))
        if len(self.updates) >= self.stop_after:
            raise KeyboardInterrupt

    def run(self, policy):
        with pytest.raises(KeyboardInterrupt):
            watch.poll(
                fetch=self.fetch,
                published_at=lambda snapshot: snapshot["publish"],
                on_update=self.on_update,
                policy=policy,
                wait=watch.silent,
                now=lambda tzinfo: start + dt.timedelta(hours=1),
            )
        return self.updates


def test_the_first_snapshot_arrives_with_no_previous():
    feed = Feed([start], stop_after=1)
    updates = feed.run(watch.WatchPolicy())

    assert updates[0][0] == 0
    assert updates[0][1] is None


def test_a_newer_publish_time_triggers_an_update():
    later = start + dt.timedelta(minutes=30)
    feed = Feed([start, later], stop_after=2)

    updates = feed.run(watch.WatchPolicy(interval_minutes=30))

    assert [u[0] for u in updates] == [0, 1]
    assert updates[1][2] == later


def test_a_late_feed_is_picked_up_by_the_retry_sequence():
    later = start + dt.timedelta(minutes=30)
    feed = Feed([start, start, start, later], stop_after=2)

    updates = feed.run(watch.WatchPolicy(retry_increments=(1, 2, 3)))

    assert updates[1][2] == later


def test_with_retry_disabled_a_late_feed_waits_for_the_next_interval():
    later = start + dt.timedelta(minutes=30)
    feed = Feed([start, start, later], stop_after=2)

    updates = feed.run(watch.WatchPolicy(retry=False))

    # The stale cycle passed without an update; the next one caught it.
    assert [u[0] for u in updates] == [0, 2]
