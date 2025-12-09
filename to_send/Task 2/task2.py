import argparse
import time
import datetime as dt
import os
import requests as rq
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots




paper_bg = "#f2e6d8"     
plot_bg = "#f2e6d8"       
grid_col = "#e3d5c6"      
axis_col = "#b0977b"      
tick_col = "#6b5a4b"      

ft_green = "#7bb274"      # muted soft green
ft_red = "#c6665c"        # muted salmon red

# Light tints for row highlighting in the table
ft_green_light = "#e3f2e1"
ft_red_light   = "#f8dad5"

def fetch_wind_solar_forecast(date, query_attempt_count=5):
    """
    Fetch day-ahead forecast generation for wind & solar (DGWS / B1440)
    for a single UTC day.

    Uses:
      GET /forecast/generation/wind-and-solar/day-ahead
      filtered by startTime via 'from' and 'to'.

    Parameters
    ----------
    date : str
        Settlement date in 'YYYY-MM-DD' (UTC).
        Query attempt: how many times to retry on failure.
    """
    base_url = "https://data.elexon.co.uk/bmrs/api/v1/forecast/generation/wind-and-solar/day-ahead"

    start_iso = f"{date}T00:00Z"
    end_iso = f"{date}T23:30Z"

    params = {
        "from": start_iso,
        "to": end_iso,
        "processType": "Day ahead",
        "format": "json",
    }

    attempt = 1
    r = None

    while attempt <= query_attempt_count:
        try:
            print(f" Forecast attempt {attempt} ...")
            r = rq.get(base_url, params=params)

            if r.status_code == 200:
                print("Forecast request OK.")
                break
            else:
                print(f"Forecast HTTP status: {r.status_code}")
        except Exception as e:
            print(f"Forecast attempt {attempt} failed: {e}")

        attempt += 1
        if attempt <= query_attempt_count:
            time.sleep(2)

    if r is None or r.status_code != 200:
        raise Exception(f"Forecast API request failed after {query_attempt_count} attempts")

    return r


def fetch_wind_solar_actuals(date, query_attempt_count=5):
    """
    Fetch actual/estimated wind & solar generation (AGWS / B1630)
    for a single UTC day.

    Uses:
      GET /generation/actual/per-type/wind-and-solar

    Parameters
    ----------
    date : str
        Settlement date in 'YYYY-MM-DD' (UTC).
        Query attempt: how many times to retry on failure.
    """
    base_url = "https://data.elexon.co.uk/bmrs/api/v1/generation/actual/per-type/wind-and-solar"

    date_obj = dt.datetime.strptime(date, "%Y-%m-%d")
    next_day = date_obj + dt.timedelta(days=1)

    start_iso = date_obj.strftime("%Y-%m-%dT00:00Z")
    end_iso = next_day.strftime("%Y-%m-%dT00:00Z")

    params = {
        "from": start_iso,
        "to": end_iso,
        "settlementPeriodFrom": 1,
        "settlementPeriodTo": 48,
        "format": "json",
    }

    attempt = 1
    r = None

    while attempt <= query_attempt_count:
        try:
            print(f" Actuals attempt {attempt} ...")
            r = rq.get(base_url, params=params)

            if r.status_code == 200:
                print(" Actuals request OK.")
                break
            else:
                print(f"Actuals HTTP status: {r.status_code}")
        except Exception as e:
            print(f"Actuals attempt {attempt} failed: {e}")

        attempt += 1
        if attempt <= query_attempt_count:
            time.sleep(2)

    if r is None or r.status_code != 200:
        raise Exception(f"Actuals API request failed after {query_attempt_count} attempts")

    return r


def forecast_req_to_df(r):
    """
    Convert forecast JSON response to a DataFrame.
    """
    data = r.json()
    return pd.DataFrame(data["data"])


def actuals_req_to_df(r):
    """
    Convert actuals JSON response to a DataFrame.
    """
    data = r.json()
    return pd.DataFrame(data["data"])


# =========================================================
#   Utility / transformation helpers
# =========================================================

def settlement_period_order():
    """
    BMRS-style ordering for a local day: 47, 48, 1..46.
    """
    return [str(sp) for sp in ([47, 48] + list(range(1, 47)))]


def normalise_mw_column(df, new_col_name):
    """
    Rename the numeric MW column to a unified name.
    Tries common candidates used in EMFIP/BMRS streams:
      - 'quantity'
      - 'generation'
      - 'value'
    """
    df = df.copy()
    candidates = ["quantity", "generation", "value"]

    src_col = None
    for c in candidates:
        if c in df.columns:
            src_col = c
            break

    if src_col is None:
        raise KeyError(
            f"Could not find an MW column in df; "
            f"looked for {candidates}, got: {list(df.columns)}"
        )

    if src_col != new_col_name:
        df = df.rename(columns={src_col: new_col_name})

    return df


def map_psr_to_fuel(psr):
    """
    Map psrType to a simple fuel label.
    """
    if psr is None:
        return None

    psr_lower = str(psr).lower()

    if "solar" in psr_lower:
        return "Solar"
    if "wind" in psr_lower:
        return "Wind"

    return None


def add_fuel_column(df):
    """
    Add a 'fuel' column (Wind / Solar) based on 'psrType'.
    """
    if df is None:
        raise ValueError("DataFrame is None in add_fuel_column().")

    df = df.copy()

    if "psrType" not in df.columns:
        raise KeyError(f"Expected 'psrType' column, got: {list(df.columns)}")

    df["fuel"] = df["psrType"].apply(map_psr_to_fuel)
    df = df[df["fuel"].notna()].reset_index(drop=True)

    return df


def convert_col_to_cest(df, col_names=("startTime",)):
    """
    Add *_cest columns for each timestamp column in col_names.
    """
    df = df.copy()
    for col in col_names:
        df[f"{col}_cest"] = (
            pd.to_datetime(df[col], utc=True)
            .dt.tz_convert("Europe/Berlin")
        )
    return df


def prepare_wind_solar_merged(forecast_df, actuals_df):
    """
    Align forecast vs actual data.

    - Add:
        forecast_MW = quantity (forecast)
        actual_MW   = quantity (actuals)
        startTime_cest via convert_col_to_cest
        fuel (Wind / Solar)
        diff_MW = actual_MW - forecast_MW
    - Aggregate per (settlementDate, settlementPeriod, fuel).
    """
    forecast_df = forecast_df.copy()
    actuals_df = actuals_df.copy()

    if "quantity" not in forecast_df.columns:
        raise KeyError(f"Forecast DF missing 'quantity'; columns: {list(forecast_df.columns)}")
    if "quantity" not in actuals_df.columns:
        raise KeyError(f"Actuals DF missing 'quantity'; columns: {list(actuals_df.columns)}")

    forecast_df["forecast_MW"] = forecast_df["quantity"]
    actuals_df["actual_MW"] = actuals_df["quantity"]

    # Timezone conversion
    forecast_df = convert_col_to_cest(forecast_df, col_names=("startTime",))
    actuals_df = convert_col_to_cest(actuals_df, col_names=("startTime",))

    if "settlementPeriod" in forecast_df.columns:
        forecast_df["settlementPeriod"] = forecast_df["settlementPeriod"].astype(int)
    if "settlementPeriod" in actuals_df.columns:
        actuals_df["settlementPeriod"] = actuals_df["settlementPeriod"].astype(int)

    # Fuel mapping
    forecast_df = add_fuel_column(forecast_df)
    actuals_df = add_fuel_column(actuals_df)

    group_cols = ["settlementDate", "settlementPeriod", "fuel"]

    missing_forecast = [c for c in group_cols if c not in forecast_df.columns]
    missing_actual = [c for c in group_cols if c not in actuals_df.columns]

    if missing_forecast:
        raise KeyError(f"Forecast DF missing {missing_forecast}")
    if missing_actual:
        raise KeyError(f"Actuals DF missing {missing_actual}")

    forecast_agg = (
        forecast_df
        .groupby(group_cols, as_index=False)
        .agg({
            "forecast_MW": "sum",
            "startTime_cest": "min",
        })
    )

    actuals_agg = (
        actuals_df
        .groupby(group_cols, as_index=False)
        .agg({
            "actual_MW": "sum",
            "startTime_cest": "min",
        })
    )

    merged = forecast_agg.merge(
        actuals_agg,
        on=group_cols,
        how="inner",
        suffixes=("_forecast", "_actual"),
    )

    merged["startTime_cest"] = merged["startTime_cest_forecast"].combine_first(
        merged["startTime_cest_actual"]
    )

    merged["diff_MW"] = merged["actual_MW"] - merged["forecast_MW"]

    merged = merged.sort_values(["settlementDate", "fuel", "settlementPeriod"]).reset_index(drop=True)

    return merged


def split_wind_solar(merged_df):
    """
    Split merged_df into separate Wind and Solar DataFrames.
    """
    merged_df = merged_df.copy()

    if "fuel" not in merged_df.columns:
        raise KeyError("'fuel' column not found in merged_df")

    df_wind = merged_df[merged_df["fuel"] == "Wind"].copy()
    df_solar = merged_df[merged_df["fuel"] == "Solar"].copy()

    for df in (df_wind, df_solar):
        if not df.empty:
            df["settlementPeriod"] = df["settlementPeriod"].astype(int)

    return df_wind, df_solar


# =========================================================
#   Plotting
# =========================================================

def plot_forecast_vs_actual_with_table(df, fuel_label="Wind", x_axis="settlementPeriod", output_dir="."):
    """
    FT-style two-row figure.

      Row 1: line + marker plot of forecast vs actual
      Row 2: table:
        settlementPeriod, forecast_MW, actual_MW, diff_MW
    """
    if df.empty:
        print(f"{fuel_label}: no data to plot.")
        return

    df = df.copy()

    if x_axis not in ("settlementPeriod", "startTime_cest"):
        raise ValueError("x_axis must be 'settlementPeriod' or 'startTime_cest'")

    # --- X values and ordering ---
    if x_axis == "settlementPeriod":
        # Local day order: 47, 48, 1..46
        order = settlement_period_order()
        df["settlementPeriod"] = df["settlementPeriod"].astype(int)
        df["settlementPeriod_str"] = df["settlementPeriod"].astype(str)

        order_index = {sp: i for i, sp in enumerate(order)}
        df["sp_sort_key"] = df["settlementPeriod_str"].map(order_index).fillna(len(order))

        df = df.sort_values("sp_sort_key").reset_index(drop=True)

        x_vals = df["settlementPeriod_str"]
        x_title = "Settlement Period"
        category_args = dict(categoryorder="array", categoryarray=order)
    else:
        df = df.sort_values("startTime_cest").reset_index(drop=True)
        x_vals = df["startTime_cest"]
        x_title = "Local start time"
        category_args = {}

    # Local date/time for title from startTime_cest (CE(S)T)
    local_dt = df["startTime_cest"].iloc[0]
    date_str = local_dt.strftime("%d %b %Y")
    tz_str = local_dt.strftime("%Z")
    title = f"{fuel_label} generation — forecast vs actual — {date_str} ({tz_str})"

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        row_heights=[0.65, 0.35],
        specs=[[{"type": "scatter"}],
               [{"type": "table"}]],
    )

    # Forecast line (FT red)
    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=df["forecast_MW"],
            mode="lines+markers",
            name=f"{fuel_label} forecast",
            marker=dict(size=7),
            line=dict(width=2, color=ft_red),
            hovertemplate=(
                f"{x_axis}: %{{x}}<br>"
                "Forecast: %{y:.1f} MW<extra></extra>"
            ),
        ),
        row=1, col=1,
    )

    # Actual line (FT green)
    fig.add_trace(
        go.Scatter(
            x=x_vals,
            y=df["actual_MW"],
            mode="lines+markers",
            name=f"{fuel_label} actual",
            marker=dict(size=7),
            line=dict(width=2, dash="dot", color=ft_green),
            hovertemplate=(
                f"{x_axis}: %{{x}}<br>"
                "Actual: %{y:.1f} MW<extra></extra>"
            ),
        ),
        row=1, col=1,
    )

    # Layout / FT styling
    fig.update_layout(
        title=title,
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        font=dict(family="Georgia, serif", color=tick_col),
        legend_title_text="Series",
        hovermode="x unified" if x_axis == "settlementPeriod" else "closest",
        margin=dict(t=60, b=40),
    )

    fig.update_traces(
        selector=dict(type="scatter"),
        marker=dict(
            size=7,
            opacity=0.9,
            line=dict(width=0),
        ),
    )

    # Axes
    fig.update_yaxes(
        title_text="Generation (MW)",
        row=1, col=1,
        gridcolor=grid_col,
        zeroline=True,
        zerolinecolor=axis_col,
        zerolinewidth=2,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
    )

    fig.update_xaxes(
        row=1, col=1,
        showgrid=False,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
        **category_args,
    )

    fig.update_xaxes(
        row=2, col=1,
        title_text=x_title,
        showgrid=False,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
        **category_args,
    )

    # Table – same ordering as df
        # Table – same ordering as df
    table_df = df[["settlementPeriod", "forecast_MW", "actual_MW", "diff_MW"]].copy()

    table_df["forecast_MW"] = table_df["forecast_MW"].round(1)
    table_df["actual_MW"] = table_df["actual_MW"].round(1)
    table_df["diff_MW"] = table_df["diff_MW"].round(1)

    # Row-wise colours based on forecast error (Actual - Forecast)
    row_colors = []
    for v in table_df["diff_MW"]:
        try:
            if pd.isna(v):
                row_colors.append(plot_bg)
            elif v >= 0:
                row_colors.append(ft_green_light)
            else:
                row_colors.append(ft_red_light)
        except Exception:
            row_colors.append(plot_bg)

    fig.add_trace(
        go.Table(
            header=dict(
                values=["SP", "Forecast (MW)", "Actual (MW)", "Actual - Forecast (MW)"],
                align="center",
                font=dict(size=12, color=paper_bg),
                fill_color=axis_col,
            ),
            cells=dict(
                values=[
                    table_df["settlementPeriod"],
                    table_df["forecast_MW"],
                    table_df["actual_MW"],
                    table_df["diff_MW"],
                ],
                align="center",
                # same row colour applied to each column
                fill_color=[row_colors, row_colors, row_colors, row_colors],
                font=dict(color=tick_col),
            ),
            columnwidth=[0.8, 1.4, 1.4, 1.6],
        ),
        row=2, col=1,
    )

    os.makedirs(output_dir, exist_ok=True)
    base = f"forecast_vs_actual_{fuel_label.lower()}_{date_str.replace(' ', '_')}"
    base = os.path.join(output_dir, base)

    try:
        fig_png = go.Figure(fig)  

        n_rows = len(table_df)
        cell_height = 20    
        header_height = 24  
        table_fraction = 0.35 

        needed_table_px = header_height + n_rows * cell_height
        fig_height = int(needed_table_px / table_fraction) + 200 

        fig_png.update_layout(width=1600, height=fig_height)
        fig_png.write_image(base + ".png", scale=2)
        print(f"Saved PNG:  {base}.png")
    except Exception as e:
        print(f"FAILED TO SAVE PNG IMAGE ({base}.png): {e}")

    
    fig.write_html(base + ".html", include_plotlyjs="cdn")
    print(f"Saved HTML: {base}.html")

    fig.show()



def print_forecast_error_summary(df, fuel_label="Wind"):
    """
    Simple stats for commentary.
    """
    if df.empty:
        print(f"{fuel_label}: no data.")
        return

    diffs = df["diff_MW"].astype(float)

    mean_err = diffs.mean()
    mae = diffs.abs().mean()
    worst_under = diffs.min()
    worst_over = diffs.max()

    print(f"{fuel_label} mean error (actual - forecast): {mean_err:.1f} MW")
    print(f"{fuel_label} mean absolute error:           {mae:.1f} MW")
    print(f"{fuel_label} max under-forecast:            {worst_under:.1f} MW")
    print(f"{fuel_label} max over-forecast:             {worst_over:.1f} MW")


# =========================================================
#   Main runner
# =========================================================

def run_part2_wind_solar(date, do_plots=True, x_axis="settlementPeriod", output_dir="."):
    """
    Fetch, align, plot, and summarise wind/solar forecast vs actuals
    for a local (Europe/Berlin) calendar day.

    Local day D (00:00–23:30) uses:
      - SP 47–48 from previous UTC settlementDate
      - SP 1–46 from the selected UTC settlementDate
    """
    print(f"Part 2 – wind & solar forecast vs actuals for local day {date}")

    # Previous day (for SP 47–48) and current day (SP 1–46)
    date_obj = dt.datetime.strptime(date, "%Y-%m-%d")
    prev_obj = date_obj - dt.timedelta(days=1)
    prev_str = prev_obj.strftime("%Y-%m-%d")

    # --- Forecasts: previous day (47–48) + current day (1–46) ---
    r_fore_prev = fetch_wind_solar_forecast(prev_str)
    r_fore_curr = fetch_wind_solar_forecast(date)

    df_fore_prev = forecast_req_to_df(r_fore_prev)
    df_fore_curr = forecast_req_to_df(r_fore_curr)

    df_fore_prev["settlementPeriod"] = df_fore_prev["settlementPeriod"].astype(int)
    df_fore_curr["settlementPeriod"] = df_fore_curr["settlementPeriod"].astype(int)

    df_fore_prev_sel = df_fore_prev[df_fore_prev["settlementPeriod"].isin([47, 48])]
    df_fore_curr_sel = df_fore_curr[df_fore_curr["settlementPeriod"].between(1, 46)]

    df_fore_local = pd.concat([df_fore_prev_sel, df_fore_curr_sel], ignore_index=True)

    # --- Actuals: previous day (47–48) + current day (1–46) ---
    r_act_prev = fetch_wind_solar_actuals(prev_str)
    r_act_curr = fetch_wind_solar_actuals(date)

    df_act_prev = actuals_req_to_df(r_act_prev)
    df_act_curr = actuals_req_to_df(r_act_curr)

    df_act_prev["settlementPeriod"] = df_act_prev["settlementPeriod"].astype(int)
    df_act_curr["settlementPeriod"] = df_act_curr["settlementPeriod"].astype(int)

    df_act_prev_sel = df_act_prev[df_act_prev["settlementPeriod"].isin([47, 48])]
    df_act_curr_sel = df_act_curr[df_act_curr["settlementPeriod"].between(1, 46)]

    df_act_local = pd.concat([df_act_prev_sel, df_act_curr_sel], ignore_index=True)

    print(f"Forecast rows (local day): {len(df_fore_local)}")
    print(f"Actual rows   (local day): {len(df_act_local)}")

    # Align, split, plot, summarise
    merged = prepare_wind_solar_merged(df_fore_local, df_act_local)
    df_wind, df_solar = split_wind_solar(merged)

    print(f"Wind rows (merged):  {len(df_wind)}")
    print(f"Solar rows (merged): {len(df_solar)}")

    if do_plots:
        plot_forecast_vs_actual_with_table(df_wind, fuel_label="Wind", x_axis=x_axis, output_dir=output_dir)
        plot_forecast_vs_actual_with_table(df_solar, fuel_label="Solar", x_axis=x_axis, output_dir=output_dir)

    print_forecast_error_summary(df_wind, fuel_label="Wind")
    print_forecast_error_summary(df_solar, fuel_label="Solar")

    return df_wind, df_solar


# =========================================================
#   CLI
# =========================================================

def parse_args():
    parser = argparse.ArgumentParser(
        description="BMRS wind & solar forecast vs actuals (Part 2).",
    )
    parser.add_argument(
        "--date",
        default="2025-11-11",
        help="Local (Europe/Berlin) calendar day (YYYY-MM-DD). "
             "Will use SP 47–48 from previous UTC day and 1–46 from this UTC day.",
    )
    parser.add_argument(
        "--x-axis",
        choices=["settlementPeriod", "startTime_cest"],
        default="settlementPeriod",
        help="X-axis variable for plots (default: settlementPeriod).",
    )
    parser.add_argument(
        "--no-plots",
        dest="do_plots",
        action="store_false",
        help="Disable plotting (just fetch & compute errors).",
    )

    parser.add_argument(
        "-o", "--output-dir",
        default=".",
        help="Directory to save output plots (default: current directory).",
    )
    parser.set_defaults(do_plots=True)

    return parser.parse_args()


def main():
    args = parse_args()

    run_part2_wind_solar(
        date=args.date,
        do_plots=args.do_plots,
        x_axis=args.x_axis,
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
