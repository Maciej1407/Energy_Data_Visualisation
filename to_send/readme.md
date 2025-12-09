
# BMRS Analysis – Task 1 & Task 2

This repository contains two scripts:

- **Task 1** (`task1.py`): day-ahead *indicated imbalance* and its evolution.
- **Task 2** (`task2.py`): *wind and solar* generation – forecast vs actuals for a local (CE(S)T) day.

Both scripts are designed to be usable from a Jupyter notebook or from the command line.


---
Quick Usage:
(optional parameters, only necessary paramter is `--date`)
```bash
python task1.py --date 2025-12-07 -o out_task1
```
- `-o` is output directory for the plots. The default is to save in the current working directory of the script
- for quick look at the most up-to date data you may run the script with --no-auto-update 
Note: If ran on a day which is not the current date the code will no longer update as it is written per the task to extract data for a single date. 

```bash
python task2.py --date 2025-11-11 --x-axis startTime_cest -o out_task2
```
- Default value for x axis for this task is settlement period, changing the x-axis is optional. 
- `-o` is also the directory within which to save the plots

## Task 1 – Indicated Imbalance (Day-Ahead)

### 1. Data handling and processing

**Goal:** for a given **UTC settlement date** `D`, build a clean local (Europe/Berlin) view of the *indicated imbalance* forecast, with exactly one value per settlement period, and then track how that forecast evolves over time.

#### 1.1 Local day logic (SP 47–48 + 1–46)

For a chosen `date` (interpreted as settlementDate `D` in UTC):

- Compute `D-1` (previous settlementDate).
- Query settlement periods as:

  - **SP 47–48 from `D-1`**
  - **SP 1–46 from `D`**

This corresponds to a single **local day** in CEST countries once we convert timestamps from UTC to CE(S)T.

The function:

```python
fetch_data(date, query_attempt_count=5)
````

* Calls
  `https://data.elexon.co.uk/bmrs/api/v1/forecast/indicated/day-ahead/evolution`
* Uses two requests with parameters:

  * `settlementDate = D-1, settlementPeriod = [47, 48]`
  * `settlementDate = D,   settlementPeriod = [1..46]`
* Wraps the HTTP calls in a small retry loop controlled by `query_attempt_count` (default 5) with a short delay between requests, this is to account for the rate-limits imposed by most public api's

#### 1.2 Response → DataFrame

```python
req_to_df(r1, r2)
```

* Converts `r1.json()["data"]` and `r2.json()["data"]` into two DataFrames.
* Concatenates them into `full_df`, which holds all records for the local day.

#### 1.3 Time conversion (UTC → CEST)

```python
convert_col_to_cest(df, col_names=["startTime", "publishTime"])
```

* For each column in `col_names`, adds a corresponding `*_cest` column, we add new columns instead of replacing original columns to preserve data and spot errors:

  ```python
  df[f"{col}_cest"] = pd.to_datetime(df[col], utc=True).dt.tz_convert("Europe/Berlin")
  ```

* This keeps the original UTC timestamps and adds CE(S)T timestamps for plotting and titles.

#### 1.4 One forecast per SP (latest publish)

The BMRS endpoint returns multiple rows per `(settlementDate, settlementPeriod)` because the forecast is republished over time.

````python
drop_na_get_final(df)
``>

- Drops rows where `indicatedImbalance` is `NaN`.
- Sorts by `publishTime_cest`.
- Uses:

  ```python
  df_valid.groupby(["settlementDate", "settlementPeriod"]).tail(1)
````

to keep the **latest** published version per settlement period.

You end up with exactly one `indicatedImbalance` per SP for the local day.

#### 1.5 Settlement period ordering

Local day ordering:

```text
47, 48, 1, 2, ..., 46
```

```python
create_custom_ordering(final_df)
```

* Casts `settlementPeriod` to `int`.
* Adds `settlementPeriod_str` (string) for categorical x-axis.
* Returns both the modified `final_df` and `order_str` (list of SPs as strings in `[47, 48, 1..46]` order).

#### 1.6 Sign of imbalance

```python
imbalance_sign(df, col="indicatedImbalance")
```

* Adds `indicatedImbalance_sign` with:

  * `"Positive"` if `indicatedImbalance >= 0`
  * `"Negative"` otherwise.

### 2. Visualisation – single snapshot

**Function:**

```python
plot(df, order_str, output_dir=".")
```

**Core idea:**

* Show the **shape of the imbalance profile across the day**, while still making positive vs negative periods visually obvious.

**Implementation:**

* Uses `plotly.express.scatter` with:

  * `x = "settlementPeriod_str"`
  * `y = "indicatedImbalance"`
  * `color = "indicatedImbalance_sign"` (Positive/Negative)
  * `category_orders={"settlementPeriod_str": order_str}`
    to enforce the `[47, 48, 1..46]` ordering on the x-axis.

* On top of the scatter, the code can be configured (in the file) to add a line trace for visual continuity across settlement periods. The important point is that **all dots for a given SP appear at the same x-position**, and the x-axis always respects the 47–48–1..46 order.

* Title is built from:

  * the **max `publishTime_cest`** – to indicate how recent the forecast is, and
  * the **settlement date** – in `"%d %b %Y"` format.

**Styling:**

* The styling is **FT-inspired by default**:

  * Warm paper background (`paper_bg`, `plot_bg`).
  * Muted FT-style greens/reds (`ft_green`, `ft_red`).
  * Soft grid lines (`grid_col`) and a serif font.

**Saving:**

Plots are saved under `output_dir` as:

* `part1_imbalance_<YYYY-MM-DD>.png` – static image via `kaleido`.
* `part1_imbalance_<YYYY-MM-DD>.html` – interactive Plotly figure.

> **Note:** PNG export requires `kaleido`. Install via:
>
> ```bash
> pip install -U kaleido
> ```

### 3. Visualisation – forecast evolution (plot_diff)

**Function:**

```python
plot_diff(prev_df, new_df, order_str, title_suffix="", output_dir=".")
```

**Goal:** Visualise how the forecast changed between two snapshots:

* `prev_df` – earlier snapshot (previous update),
* `new_df` – later snapshot (latest update).

The function is also used by the auto-update loop.

#### 3.1 Merging logic

Because forecasts can be compared:

* within the **same settlement date**, or
* across **different settlement dates** (e.g. crossing midnight),

the function handles both cases.

Steps:

1. Rename `indicatedImbalance` columns to:

   * `indicatedImbalance_prev`
   * `indicatedImbalance_new`

2. Determine merge keys:

   * If both snapshots share a single identical `settlementDate`, merge on:

     ```python
     merge_on = ["settlementDate", "settlementPeriod"]
     ```

     and set `is_same_date = True`.

   * Otherwise, merge on:

     ```python
     merge_on = ["settlementPeriod"]
     ```

     and set `is_same_date = False`.

3. After merging:

   * Compute `delta = indicatedImbalance_new - indicatedImbalance_prev`.
   * Derive `sign_prev` and `sign_new` columns (Positive/Negative).
   * Keep `settlementPeriod_str` and use `order_str` to enforce the desired x-ordering.

#### 3.2 Plot elements

The design is consistent with the testing helper (`test_plot_diff_different_dates`):

* **Previous forecast:**

  * Two marker traces:

    * Light green for previous positive values.
    * Light red for previous negative values.
  * Small markers, semi-transparent.

* **Latest forecast:**

  * Two marker traces:

    * Bold green for latest positive values.
    * Bold red for latest negative values.
  * Larger markers with a thin darker outline.

* **Vertical dotted lines per SP:**

  * For settlement periods where both previous and latest values exist:

    * Draw a vertical line from `previous` to `latest`.
    * Colour:

      * Green if `delta > 0` (imbalance increased).
      * Red otherwise.
    * Style: `dash="dot"` with moderate line width.
    * `hoverinfo="skip"` so these lines do not pollute hover tooltips.

#### 3.3 Hover behaviour

To avoid the issue where multiple traces on the same x-value produce a combined hover box, the function uses:

```python
hovermode="closest"
```

Each point (previous/latest) has its own tooltip, e.g.:

* `"Previous<br>SP: ...<br>Imbalance: ..."`
* `"Latest<br>SP: ...<br>Imbalance: ..."`

#### 3.4 Titles and saving

Titles use:

* `prev_df["publishTime_cest"].max()` and
* `new_df["publishTime_cest"].max()`

to show previous vs latest times, and settlement dates where possible. The optional `title_suffix` is used to label the update cycle (e.g. `"Update 1"`, `"Update 2 (Retry)"`), especially in the auto-update loop.

Files are saved as:

* `part1_diff_<YYYY-MM-DD>_<timestamp>.png`
* `part1_diff_<YYYY-MM-DD>_<timestamp>.html`

under `output_dir`.

### 4. Auto-update loop

**Function:**

```python
auto_update_loop(
    date,
    update_interval_minutes=30,
    retry=True,
    retry_increments=(30, 60, 120),
    output_dir="."
)
```

**Purpose:** Monitor a given day’s indicated imbalance forecast and repeatedly visualise updates.

**Behaviour:**

1. **Initial snapshot:**

   * Calls `full_run_and_plot(date, do_plot=True, output_dir=output_dir)` to:

     * fetch data,
     * compute final per-SP forecast,
     * plot the base profile.
   * Reads the latest `publishTime_cest` from the resulting DataFrame.

2. **Expected next update:**

   ```python
   next_expected = prev_max_publish + timedelta(minutes=update_interval_minutes)
   ```

3. **Countdown:**

   * Uses a simple `countdown_timer(seconds)` that prints the remaining time to stdout, then checks again at (or shortly after) `next_expected`.

4. **Update check:**

   * Fetches a new snapshot (without plotting).
   * If `new_max_publish > prev_max_publish`:

     * Calls `plot_diff(prev_df, new_df, order_str, title_suffix=f"Update {cycle}", output_dir=output_dir)`.
     * Replaces `prev_df` and `prev_max_publish` with the new values.

5. **Retries (optional, but highly reccomended):**

Though data may be uploaded by the provider precisely on time, updates of the database from which the public api is queryed often encounters a delay. In my personal testing there is an average of 60-180 second delay between the `publish_date` and the actual time the data is available 
   * If no new data is found and `retry=True`:

     * Loops over `retry_increments` (e.g. 30s, 60s, 120s).
     * After each wait, fetches again and checks if a new publish appears.
   * If still no new data, reports and waits for the next main interval.

This gives a simple way to watch the forecast evolve over the day, with each update visualised against the previous one.

### 5. Task 1 – CLI usage

Example commands:

```bash
# Basic run for a single day (UTC settlement date)
python task1.py --date 2025-11-11

# Run with a custom output directory for plots
python task1.py --date 2025-11-11 --output-dir plots/task1

# Run auto-update loop with different intervals
python task1.py --date 2025-11-11 --update-interval-minutes 20
```

Typical arguments:

* `--date YYYY-MM-DD`
  Settlement date (UTC) for which to build the local-day imbalance view.

* `--update-interval-minutes N`
  Approximate time between forecast updates (default 30).

* `--no-retry` or similar flag
  Disable the short retry sequence if there is no new data at the expected time.

* `-o, --output-dir PATH`
  Directory to save PNG/HTML plots (created if it does not exist).

---

## Task 2 – Wind & Solar: Forecast vs Actuals

### 1. Data handling and processing

**Goal:** for a given local day in Europe/Berlin, compare **day-ahead forecast** vs **actual/estimated generation** for **wind** and **solar**, aligned by settlement period and fuel type.

### 1.1 Local day construction

Same local-day logic as Task 1:

* For settlementDate `D` (UTC):

  * **SP 47–48 from `D-1`**
  * **SP 1–46 from `D`**
* These are concatenated into a single local-day DataFrame for each of:

  * forecast (wind & solar),
  * actuals (wind & solar).

The functions:

```python
fetch_wind_solar_forecast(date, query_attempt_count=5)
fetch_wind_solar_actuals(date, query_attempt_count=5)
```

wrap the relevant BMRS endpoints in retry loops:

* Forecast:
  `https://data.elexon.co.uk/bmrs/api/v1/forecast/generation/wind-and-solar/day-ahead`
* Actuals:
  `https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type/wind-and-solar`

Parsing is handled by:

```python
forecast_req_to_df(r)
actuals_req_to_df(r)
```

which simply call `response.json()["data"]` into DataFrames.

### 1.2 Fuel identification and MW values

**Fuel mapping:**

```python
add_fuel_column(df)
```

* Uses `psrType`:

  * `"Wind"` if the string contains `"wind"`,
  * `"Solar"` if the string contains `"solar"`,
  * drops everything else.

**MW values:**

* Forecast: `forecast_MW = quantity`
* Actuals:  `actual_MW   = quantity`

### 1.3 Aggregation and merge

```python
prepare_wind_solar_merged(forecast_df, actuals_df)
```

* Applies `convert_col_to_cest(..., col_names=("startTime",))` to both datasets.

* Ensures `settlementPeriod` is an integer.

* Defines grouping columns:

  ```python
  group_cols = ["settlementDate", "settlementPeriod", "fuel"]
  ```

* Forecast:

  ```python
  forecast_agg = forecast_df.groupby(group_cols, as_index=False).agg({
      "forecast_MW": "sum",
      "startTime_cest": "min",
  })
  ```

* Actuals similarly aggregated for `actual_MW`.

* Merges forecast and actuals on `group_cols` (`how="inner"`).

* Chooses `startTime_cest` using `combine_first`.

* Calculates `diff_MW = actual_MW - forecast_MW`.

* Sorts the result by `settlementDate`, `fuel`, `settlementPeriod`.

**Splitting by fuel:**

```python
df_wind, df_solar = split_wind_solar(merged_df)
```

* Returns separate DataFrames for wind and solar, with settlement periods as integers.

### 2. Visualisation – forecast vs actuals (per fuel)

**Function:**

```python
plot_forecast_vs_actual_with_table(
    df,
    fuel_label="Wind",
    x_axis="settlementPeriod",
    output_dir="."
)
```

**Purpose:** For each of wind and solar:

* Show forecast vs actual generation for the local day.
* Provide a small numeric table underneath the chart.

#### 2.1 X-axis options

* Default: `x_axis="settlementPeriod"`:

  * Uses the `[47, 48, 1..46]` ordering via `settlement_period_order()`.
  * Adds a sort key column so the DataFrame and table follow that order.

* Alternative: `x_axis="startTime_cest"`:

  * Uses actual local timestamps on the x-axis.
  * Sorted by `startTime_cest`.

#### 2.2 Subplot layout

Two-row `make_subplots` layout:

* **Row 1:** line+marker plot:

  * Forecast vs actual generation (MW).
  * Forecast: solid line, markers.
  * Actual: dotted line, markers.

* **Row 2:** table displaying:

  * Settlement period.
  * Forecast (MW).
  * Actual (MW).
  * Difference = actual – forecast (MW).

Only one DataFrame (wind or solar) is passed at a time, so each figure is dedicated to a single fuel.

#### 2.3 Visual style

* The theme is **FT-style by default** (same palette as Task 1):

  * Warm background (`paper_bg`, `plot_bg`).
  * Serif font.
  * `ft_red` for forecast, `ft_green` for actual on the top plot.
  * Table header uses the same axis colour, with body cells matching the background.

There is no style toggle in the current code; all Task 2 plots use this palette.

#### 2.4 Saving

For a local date `DD Mon YYYY`, files are saved under `output_dir` as:

* `forecast_vs_actual_wind_<DD_Mmm_YYYY>.png`
* `forecast_vs_actual_wind_<DD_Mmm_YYYY>.html`

and equivalently for solar.

### 3. Error summary and system commentary

**Function:**

```python
print_forecast_error_summary(df, fuel_label="Wind")
```

For each fuel, it prints:

* Mean error `E[actual - forecast]` (signed).
* Mean absolute error (MAE).
* Maximum under-forecast (most negative `diff_MW`).
* Maximum over-forecast (most positive `diff_MW`).

Example format:

```text
Wind mean error (actual - forecast):  -85.3 MW
Wind mean absolute error:            210.4 MW
Wind max under-forecast:             -640.0 MW
Wind max over-forecast:               390.2 MW
```

These numbers support a short qualitative commentary, for example:

* **Under-forecast periods** (actual > forecast):

  * System receives more wind/solar than expected.
  * Tends to increase downward balancing actions (curtailment, reducing other generation).
  * Can depress imbalance prices if the surplus is significant.

* **Over-forecast periods** (actual < forecast):

  * System sees less wind/solar than planned.
  * Requires upward balancing (ramping conventional generation, imports, reserve).
  * Puts upward pressure on imbalance prices and reduces system margin.

The combination of the plots and these statistics makes it easy to see not just that “the forecast was wrong”, but **when** and **in which direction** the system was exposed.

### 4. Task 2 – CLI usage

Typical usage patterns:

```bash
# Basic run for a local day based on settlementDate D
python task2.py --date 2025-11-11

# Save outputs into a custom folder
python task2.py --date 2025-11-11 --output-dir plots/task2

# Use local time on the x-axis instead of settlementPeriod
python task2.py --date 2025-11-11 --x-axis startTime_cest
```

Common arguments:

* `--date YYYY-MM-DD`
  Settlement date (UTC). The script automatically pulls SP 47–48 from the previous day and SP 1–46 from this date to form the local day.

* `--x-axis {settlementPeriod,startTime_cest}`
  Choose between settlement period index and local time on the x-axis.

* `--no-plots`
  Skip plotting and only perform data processing and print error summaries.

* `-o, --output-dir PATH`
  Directory for PNG/HTML output (created when missing).

All of the underlying functions (fetch, processing, plotting) can also be called directly from a jupyter notebook for more interactive exploration.
