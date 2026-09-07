# BMRS Analysis – Task 1 & Task 2

Fetch, normalise, and plot GB electricity market data from the Elexon BMRS
Insights API.

- **Task 1** (`task1.py`): day-ahead *indicated imbalance* and how the forecast evolves.
- **Task 2** (`task2.py`): *wind and solar* generation – forecast vs actuals for a local (CE(S)T) day.

Both scripts still work exactly as they did. The logic behind them now lives in
the `energyviz` package, so it can also be driven from a notebook, extended to a
second data source, or written out as Parquet.

---

## Quick usage

The only required parameter is `--date`.

```bash
python task1.py --date 2025-12-07 -o out_task1
```

```bash
python task2.py --date 2025-11-11 --x-axis start_local -o out_task2
```

Flags shared by both:

| Flag | Meaning |
|---|---|
| `--date` | Settlement date `D` (YYYY-MM-DD). The local day is SP 47–48 from `D-1` plus SP 1–46 from `D`. |
| `-o`, `--output-dir` | Where to save plots (default: current directory). |
| `--parquet-dir` | Also write the normalised data as Parquet under this directory. |
| `--timezone` | Local zone for the day view (default `Europe/Berlin`). |
| `--no-show` | Do not open the figures in a browser. |
| `--quiet` | Only report warnings and errors. |

Task 1 additionally takes `--update-interval-minutes`, `--retry-increments`,
`--no-retry`, and `--no-auto-update` (plot the latest data once and exit).
Task 2 additionally takes `--x-axis` and `--no-plots`.

Running the modules directly works too, and is the same thing without the
default date:

```bash
python -m cli.imbalance --date 2025-12-07 --no-auto-update -o out_task1
```

### Setup

```bash
python3 -m venv mvenv && ./mvenv/bin/pip install -r requirements.txt
```

---

## How it is put together

The centre of the design is not the plots, it is a **single normalised frame
schema** that every source produces and every consumer reads. Get one row of
data into that shape and it can be transformed, plotted, and written to Parquet
without anything downstream knowing which API it came from.

```
                       ┌──────────────┐
   BMRS  ──▶ sources ──▶│  normalised  │──▶ domain ──▶ viz ──▶ png / html
  (ENTSOE later)        │    frame     │──▶ store  ──▶ parquet
                        └──────────────┘
                         energyviz.schema
```

| Module | Responsibility |
|---|---|
| `energyviz/config.py` | `Settings` – base URL, timezone, retry policy, timeout. Constructed once at the CLI boundary and passed down. |
| `energyviz/schema.py` | **The contract.** Column names, dtypes, vocabulary, `conform`, `validate`, `select`. |
| `energyviz/transport.py` | The one HTTP retry loop. Sleep is injected, so tests do not wait. |
| `energyviz/sources/elexon.py` | Endpoint table, BMRS payloads → normalised frames. |
| `energyviz/domain/settlement.py` | The 47, 48, 1–46 local-day rule; period ordering; UTC → local. |
| `energyviz/domain/imbalance.py` | Latest forecast per period, sign labelling, snapshot diffing. |
| `energyviz/domain/windsolar.py` | Forecast/actual alignment and error statistics. |
| `energyviz/viz/theme.py` | The FT-style palette, as a frozen dataclass rather than module globals. |
| `energyviz/viz/figures.py` | Frame → `plotly.Figure`. Pure: no I/O, no `.show()`. |
| `energyviz/viz/export.py` | The only module that writes plot files. |
| `energyviz/store/parquet.py` | Normalised frame ↔ Parquet tree. |
| `energyviz/watch.py` | Poll a feed until it publishes something new. Clock injected. |
| `cli/` | Argument parsing and wiring. No logic. |

Three rules hold throughout, and they are what make the rest work:

1. **Nothing below `cli/` prints, writes, or reads the clock on its own.** Those
   are passed in. `viz/export.py` writes because writing is its whole job.
2. **Errors are raised, not printed.** `FetchError` and `SchemaError` travel up
   to `cli/`, which decides what to do about them.
3. **Data is carried in frozen dataclasses**, not dicts – `Settings`,
   `RetryPolicy`, `Theme`, `Endpoint`, `WatchPolicy`, `ErrorSummary`.

### The normalised frame

```
source             str        "elexon"
dataset            str        "imbalance_forecast" | "generation_forecast" | "generation_actual" | "day_ahead_price"
zone               str        "GB", "PL", ...
settlement_date    str        the date the upstream API files the row under
settlement_period  Int64      1..48; NA for sources that have no periods
start_utc          ts[UTC]    start of the delivery interval
start_local        ts[tz]     the same instant in Settings.timezone
publish_utc        ts[UTC]    when upstream published it; NA where not reported
series             str        "imbalance" | "generation" | "demand" | "margin" | "wind" | "solar" | "price"
value              float64
unit               str        "MW" | "EUR/MWh"
```

It is a long-format **observation log**, not a deduplicated cube: one row per
observation the API actually reported. Wind arrives split into onshore and
offshore, and an evolving forecast arrives once per publish time, so a single
`(dataset, date, period, series)` may appear several times. Collapsing that is
`domain`'s job, not the fetcher's.

`settlement_period` is nullable on purpose. It is what lets an hourly source
that has no concept of settlement periods into the same schema without changing
it.

---

## Extending it

### A second data source

Write one module and add one line. The rest of the package does not change.

```python
# energyviz/sources/entsoe.py
source_name = "entsoe"

endpoints = {
    "day_ahead_prices": Endpoint("/api", schema.day_ahead_price, unit="EUR/MWh"),
}

class EntsoeClient:
    def __init__(self, http, settings):
        ...

    def fetch_day_ahead_prices(self, zone, start, end):
        payload = self.http.get_json(url, params, label=...)
        return schema.conform(...)     # ← the only contract to meet
```

```python
# energyviz/sources/__init__.py
builders = {
    elexon.source_name: elexon.ElexonClient,
    entsoe.source_name: entsoe.EntsoeClient,     # ← the one line
}
```

ENTSOE is hourly, so its rows carry `settlement_period = NA` and join on
`start_utc`. Its vocabulary (`day_ahead_price`, `price`, `"EUR/MWh"`) is already
in `schema.py`, and every existing transform and Parquet path works on it
unchanged.

That claim is checked rather than asserted. `tests/test_second_source.py`
implements an ENTSOE day-ahead price source against a real A44 payload — XML
rather than JSON, hourly rather than half-hourly, priced in EUR/MWh rather than
measured in MW, with no settlement periods at all — and runs it through the same
schema, registry, and store as Elexon. If a change here makes a second source
harder to add, that test fails.

### A second theme

The palette is a value, so no plot function needs editing:

```python
from energyviz.viz import figures, theme

midnight = theme.Theme(
    paper_bg="#12161c", plot_bg="#12161c",
    grid="#232a33", axis="#4a5866", tick="#c8d2dc",
    green="#4ade80", red="#f87171",
    font_family="Inter, system-ui, sans-serif",
)

figures.imbalance_snapshot(snapshot, "Europe/Berlin", theme=midnight).show()
```

### Parquet

```bash
python task2.py --date 2025-11-11 --parquet-dir data --no-plots
```

```
data/generation_forecast/GB/2025-11-11.parquet
data/generation_actual/GB/2025-11-11.parquet
```

One file per dataset, zone and settlement date, so re-fetching a day is a single
overwrite and a whole dataset is readable by globbing. Because the *schema* is
the contract rather than the pandas code, a reader in another language reads the
same columns with the same meanings – which is the intended path for moving the
fetch and store layers to Go later.

---

## Testing

```bash
./mvenv/bin/python -m pytest tests -q
```

65 tests, no network: `tests/fixtures/` holds recorded BMRS payloads and
`tests/support.py` serves them in place of `HttpClient`. The retry loop, the
poll loop, and the figure builders are all exercised without a socket, a wait,
or a browser.

`tests/golden/` holds the output of the **pre-refactor** `task1.py` and
`task2.py` over those same payloads, and `tests/test_golden.py` pins the current
code to it. The refactor reproduces the old numbers exactly – 48 imbalance rows,
48 wind and 48 solar rows, and the same error statistics.

To re-record the fixtures against live BMRS, delete `tests/fixtures/*.json` and
re-run the recorder described at the top of `tests/support.py`.

---

## Notes on the refactor

Behaviour is preserved, with four exceptions – all of them defects in the
original:

- **The snapshot filename used the wrong date.** `part1_imbalance_*` took its
  date from the first row of the frame, which is SP 47 and therefore belongs to
  `D-1`. Running `--date 2025-12-07` wrote `part1_imbalance_2025-12-06.png`. It
  now uses the date you asked for.
- **The PNG was written twice.** The first write had no dimensions and was
  immediately overwritten by a second at 1600×900. Now written once.
- **`plot_diff` dropped `--output-dir` on one path.** The first-attempt call
  omitted it, so a revision found on the first check was written to the working
  directory instead of the chosen one. Both paths now pass it.
- **`--no-auto-update` was documented but never implemented.** It exists now.

One quirk was kept deliberately: because a local day spans two settlement dates,
the diff plot always matches snapshots on settlement period alone rather than on
`(date, period)`. That is the correct behaviour here, and it is what the original
did too.

`convert_col_to_cest`, the settlement-period ordering, the FT palette, the save
block and the retry loop each existed in two or three copies across `task1.py`,
`task2.py` and the notebook. Each now exists once. `normalise_mw_column` was
dead and is gone.
