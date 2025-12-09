import time
import datetime as dt
import argparse
import os
import requests as rq
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# =========================
# FT-style colour palette
# =========================

paper_bg = "#f2e6d8"      # FT-ish warm paper
plot_bg = "#f2e6d8"
grid_col = "#e3d5c6"      # soft grid
axis_col = "#b0977b"      # axis lines
tick_col = "#6b5a4b"      # text

ft_green = "#7bb274"      # muted soft green
ft_red = "#c6665c"        # muted salmon red
ft_light_green = "#b6d6b0"
ft_light_red = "#e2a39b"


# =========================
# Core data functions
# =========================

def fetch_data(date, query_attempt_count=5):
    """
    Fetch:
      - SP 47–48 from previous UTC settlementDate
      - SP 1–46 from selected UTC settlementDate
    for indicated day-ahead imbalance evolution.
    """
    attempt = 1
    base_url = "https://data.elexon.co.uk/bmrs/api/v1/forecast/indicated/day-ahead/evolution"

    datetime_obj = dt.datetime.strptime(date, "%Y-%m-%d")
    last_day = datetime_obj - dt.timedelta(days=1)
    last_day_str = last_day.strftime("%Y-%m-%d")

    last_two_p = [47, 48]
    settlement_periods_curr = list(range(1, 47))

    params1 = {
        "settlementDate": last_day_str,
        "settlementPeriod": last_two_p,
        "format": "json",
    }
    params2 = {
        "settlementDate": date,
        "settlementPeriod": settlement_periods_curr,
        "format": "json",
    }

    r1, r2 = None, None

    while attempt <= query_attempt_count:
        try:
            r1 = rq.get(base_url, params=params1)
            time.sleep(1)
            r2 = rq.get(base_url, params=params2)
            if r1.status_code == 200 and r2.status_code == 200:
                break
        except Exception as e:
            print(f"Attempt {attempt} failed: {e}. Retrying...")

        attempt += 1
        if attempt <= query_attempt_count:
            time.sleep(2)

    if r1 is None or r2 is None or r1.status_code != 200 or r2.status_code != 200:
        raise Exception(f"API request failed after {query_attempt_count} attempts")

    return r1, r2


def req_to_df(r1, r2):
    data1 = r1.json()
    data2 = r2.json()

    p1 = pd.DataFrame(data1["data"])
    p2 = pd.DataFrame(data2["data"])

    full_df = pd.concat([p1, p2], ignore_index=True)
    return full_df


def convert_col_to_cest(df, col_names=["startTime", "publishTime"]):
    df = df.copy()
    for col in col_names:
        df[f"{col}_cest"] = (
            pd.to_datetime(df[col], utc=True)
            .dt.tz_convert("Europe/Berlin")
        )
    return df


def drop_na_get_final(df):
    df_valid = df.dropna(subset=["indicatedImbalance"]).copy()
    df_valid = df_valid.sort_values("publishTime_cest")
    final_df = (
        df_valid
        .groupby(["settlementDate", "settlementPeriod"])
        .tail(1)
        .reset_index(drop=True)
    )
    return final_df


def create_custom_ordering(final_df):
    order = [47, 48] + list(range(1, 47))
    final_df = final_df.copy()
    final_df["settlementPeriod"] = final_df["settlementPeriod"].astype(int)
    final_df["settlementPeriod_str"] = final_df["settlementPeriod"].astype(str)
    order_str = list(map(str, order))
    return final_df, order_str


def imbalance_sign(df, col="indicatedImbalance"):
    df = df.copy()
    df[col + "_sign"] = df[col].apply(
        lambda x: "Positive" if x >= 0 else "Negative"
    )
    return df


# =========================
# Plot helpers
# =========================

def plot(df, order_str, output_dir="."):
    df = df.copy()

    latest_publish = df["publishTime_cest"].max()
    main_date = pd.to_datetime(df["settlementDate"]).max()

    date_str = main_date.strftime("%d %b %Y")
    time_str = latest_publish.strftime("%H:%M %Z")
    title = f"Indicated Imbalance per Settlement Period — {date_str}, {time_str}"

    fig = px.scatter(
        df,
        x="settlementPeriod_str",
        y="indicatedImbalance",
        title=title,
        category_orders={"settlementPeriod_str": order_str},
        color="indicatedImbalance_sign",
        color_discrete_map={
            "Positive": ft_green,
            "Negative": ft_red,
        },
        labels={
            "settlementPeriod_str": "Settlement Period",
            "indicatedImbalance": "Indicated Imbalance (MW)",
            "indicatedImbalance_sign": "Imbalance Sign",
        },
    )

    # FT styling
    fig.update_traces(
        marker=dict(
            size=8,
            opacity=0.9,
            line=dict(width=0),
        )
    )

    fig.update_layout(
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        font=dict(family="Georgia, serif", color=tick_col),
    )

    fig.update_xaxes(
        showgrid=False,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
    )

    fig.update_yaxes(
        gridcolor=grid_col,
        zeroline=True,
        zerolinecolor=axis_col,
        zerolinewidth=2,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
    )

    # Save PNG + HTML
    try:
        date_val = pd.to_datetime(df["settlementDate"].iloc[0])
        date_str_file = date_val.strftime("%Y-%m-%d")
    except Exception:
        date_str_file = "unknown_date"

    base = f"part1_imbalance_{date_str_file}"
    base = os.path.join(output_dir, base)

    try:
        fig.write_image(base + ".png")
        print(f"Saved PNG:  {base}.png")
    except Exception as e:
        print(f"FAILED TO SAVE PNG IMAGE ({base}.png): {e}")

    fig.write_image(base + ".png", width=1600, height=900, scale=2)
    fig.write_html(base + ".html", include_plotlyjs="cdn")
    print(f"Saved HTML: {base}.html")

    fig.show()


def full_run_and_plot(date, do_plot=True, output_dir = "."):
    r1, r2 = fetch_data(date)
    df_raw = req_to_df(r1, r2)
    df_raw = convert_col_to_cest(df_raw)
    final_df = drop_na_get_final(df_raw)
    final_df, order_str = create_custom_ordering(final_df)
    final_df = imbalance_sign(final_df)

    if do_plot:
        plot(final_df, order_str, output_dir=output_dir)

    return final_df


def plot_diff(prev_df, new_df, order_str, title_suffix="", output_dir ="."):
    # Plot the difference between previous and new forecast versions
    prev_df = prev_df.copy()
    new_df = new_df.copy()

    # Rename for clarity
    prev_df = prev_df.rename(columns={"indicatedImbalance": "indicatedImbalance_prev"})
    new_df = new_df.rename(columns={"indicatedImbalance": "indicatedImbalance_new"})

    # Dates present in each snapshot
    prev_dates = prev_df["settlementDate"].unique()
    new_dates = new_df["settlementDate"].unique()

    # Decide merge key: same date vs different date(s)
    if len(prev_dates) == 1 and len(new_dates) == 1 and prev_dates[0] == new_dates[0]:
        merge_on = ["settlementDate", "settlementPeriod"]
        is_same_date = True
    else:
        merge_on = ["settlementPeriod"]
        is_same_date = False

    merged = prev_df.merge(
        new_df,
        on=merge_on,
        how="outer",
        suffixes=("_prev", "_new"),
    )

    # Ensure types and SP labels
    merged["settlementPeriod"] = merged["settlementPeriod"].astype(int)
    merged["settlementPeriod_str"] = merged["settlementPeriod"].astype(str)

    # Compute delta and signs
    merged["delta"] = merged["indicatedImbalance_new"] - merged["indicatedImbalance_prev"]

    merged["sign_new"] = merged["indicatedImbalance_new"].apply(
        lambda x: "Positive" if pd.notna(x) and x >= 0
        else "Negative" if pd.notna(x)
        else None
    )
    merged["sign_prev"] = merged["indicatedImbalance_prev"].apply(
        lambda x: "Positive" if pd.notna(x) and x >= 0
        else "Negative" if pd.notna(x)
        else None
    )

    # Masks for alignment
    prev_mask = merged["indicatedImbalance_prev"].notna()
    new_mask = merged["indicatedImbalance_new"].notna()
    both_mask = prev_mask & new_mask

    fig = go.Figure()

    # Previous points (faded) – FT style
    prev_positive_mask = prev_mask & (merged["sign_prev"] == "Positive")
    prev_negative_mask = prev_mask & (merged["sign_prev"] == "Negative")

    if prev_positive_mask.any():
        fig.add_trace(go.Scatter(
            x=merged.loc[prev_positive_mask, "settlementPeriod_str"],
            y=merged.loc[prev_positive_mask, "indicatedImbalance_prev"],
            mode="markers",
            name="Previous (Positive)",
            marker=dict(size=8, color=ft_light_green, opacity=0.7),
            showlegend=True,
            hovertemplate=(
                "Settlement Period: %{x}<br>"
                "Indicated Imbalance (MW): %{y}"
                "<extra>Previous</extra>"
            ),
        ))

    if prev_negative_mask.any():
        fig.add_trace(go.Scatter(
            x=merged.loc[prev_negative_mask, "settlementPeriod_str"],
            y=merged.loc[prev_negative_mask, "indicatedImbalance_prev"],
            mode="markers",
            name="Previous (Negative)",
            marker=dict(size=8, color=ft_light_red, opacity=0.7),
            showlegend=True,
            hovertemplate=(
                "Settlement Period: %{x}<br>"
                "Indicated Imbalance (MW): %{y}"
                "<extra>Previous</extra>"
            ),
        ))

    # New Points (bold)
    new_positive_mask = new_mask & (merged["sign_new"] == "Positive")
    new_negative_mask = new_mask & (merged["sign_new"] == "Negative")

    if new_positive_mask.any():
        fig.add_trace(go.Scatter(
            x=merged.loc[new_positive_mask, "settlementPeriod_str"],
            y=merged.loc[new_positive_mask, "indicatedImbalance_new"],
            mode="markers",
            name="Latest (Positive)",
            marker=dict(size=12, color=ft_green, opacity=0.95,
                        line=dict(width=1, color="#3f6b39")),
            showlegend=True,
            hovertemplate=(
                "Settlement Period: %{x}<br>"
                "Indicated Imbalance (MW): %{y}"
                "<extra>Latest</extra>"
            ),
        ))

    if new_negative_mask.any():
        fig.add_trace(go.Scatter(
            x=merged.loc[new_negative_mask, "settlementPeriod_str"],
            y=merged.loc[new_negative_mask, "indicatedImbalance_new"],
            mode="markers",
            name="Latest (Negative)",
            marker=dict(size=12, color=ft_red, opacity=0.95,
                        line=dict(width=1, color="#7c2f28")),
            showlegend=True,
            hovertemplate=(
                "Settlement Period: %{x}<br>"
                "Indicated Imbalance (MW): %{y}"
                "<extra>Latest</extra>"
            ),
        ))

    # Line between old and new points
    if both_mask.any():
        for _, row in merged[both_mask].iterrows():
            if pd.notna(row["indicatedImbalance_prev"]) and pd.notna(row["indicatedImbalance_new"]):
                color = ft_green if row["delta"] > 0 else ft_red
                fig.add_trace(go.Scatter(
                    x=[row["settlementPeriod_str"], row["settlementPeriod_str"]],
                    y=[row["indicatedImbalance_prev"], row["indicatedImbalance_new"]],
                    mode="lines",
                    line=dict(color=color, width=2, dash="dot"),
                    showlegend=False,
                    hoverinfo="skip",
                ))

    # Build title
    prev_publish = prev_df["publishTime_cest"].max()
    new_publish = new_df["publishTime_cest"].max()

    prev_time_str = prev_publish.strftime("%H:%M %Z")
    new_time_str = new_publish.strftime("%H:%M %Z")

    if is_same_date:
        main_date = pd.to_datetime(prev_dates[0])
        date_str = main_date.strftime("%d %b %Y")
        base_title = f"Imbalance per Settlement Period {date_str}: {prev_time_str} vs {new_time_str}"
    else:
        prev_date = pd.to_datetime(prev_dates.max())
        new_date = pd.to_datetime(new_dates.max())
        prev_date_str = prev_date.strftime("%d %b %Y")
        new_date_str = new_date.strftime("%d %b %Y")
        base_title = (
            f"Imbalance per Settlement Period "
            f"{prev_date_str} {prev_time_str} vs {new_date_str} {new_time_str}"
        )

    if title_suffix:
        base_title = f"{base_title} ({title_suffix})"

    fig.update_layout(
        title=base_title,
        paper_bgcolor=paper_bg,
        plot_bgcolor=plot_bg,
        font=dict(family="Georgia, serif", color=tick_col),
        hovermode="closest",
        legend_title_text="Forecast version",
    )

    fig.update_xaxes(
        categoryorder="array",
        categoryarray=order_str,
        title="Settlement Period",
        showgrid=True,
        gridwidth=1,
        gridcolor=grid_col,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
    )

    fig.update_yaxes(
        title="Indicated Imbalance (MW)",
        showgrid=True,
        gridwidth=1,
        gridcolor=grid_col,
        zeroline=True,
        zerolinewidth=2,
        zerolinecolor=axis_col,
        linecolor=axis_col,
        tickfont=dict(color=tick_col),
    )

    # Save PNG + HTML
    try:
        if "settlementDate" in new_df.columns:
            date_val = pd.to_datetime(new_df["settlementDate"].iloc[0])
            date_str_file = date_val.strftime("%Y-%m-%d")
        else:
            date_str_file = "unknown_date"
    except Exception:
        date_str_file = "unknown_date"

    time_str_file = new_publish.strftime("%Y%m%dT%H%M%S")
    base = f"part1_diff_{date_str_file}_{time_str_file}"
    base = os.path.join(output_dir, base)

    try:
        fig.write_image(base + ".png")
        print(f"Saved PNG:  {base}.png")
    except Exception as e:
        print(f"FAILED TO SAVE PNG IMAGE ({base}.png): {e}")

    fig.write_image(base + ".png", width=1600, height=900, scale=2)
    fig.write_html(base + ".html", include_plotlyjs="cdn")
    print(f"Saved HTML: {base}.html")

    fig.show()


# =========================
# Auto-update machinery
# =========================

def countdown_timer(seconds):
    # Countdown Timer to inform user of query attempts
    while seconds:
        mins, secs = divmod(seconds, 60)
        hours, mins = divmod(mins, 60)
        timeformat = f" Next update in: {hours:02d}:{mins:02d}:{secs:02d}"
        print(timeformat, end="\r", flush=True)
        time.sleep(1)
        seconds -= 1
    print(" Checking for new data..." + " " * 30)


def auto_update_loop(
    date,
    update_interval_minutes=30,
    retry=True,
    retry_increments=(30, 60, 120),
    output_dir = "."
):
    # Code to automatically update and plot new data as it becomes available
    print(f" Starting auto-update loop for settlement date: {date}")

    # Initial snapshot and plot
    print(" Fetching and plotting initial data...")
    prev_df = full_run_and_plot(date, do_plot=True, output_dir=output_dir)
    prev_df, order_str = create_custom_ordering(prev_df)
    prev_max_publish = prev_df["publishTime_cest"].max()
    print(f" Initial latest publishTime_cest: {prev_max_publish}")

    update_cycle = 1

    while True:
        next_expected = prev_max_publish + dt.timedelta(minutes=update_interval_minutes)

        now = dt.datetime.now(tz=next_expected.tzinfo)
        seconds_to_wait = (next_expected - now).total_seconds()

        if seconds_to_wait > 0:
            minutes_to_wait = seconds_to_wait / 60
            print(
                f"\n Update cycle {update_cycle}: "
                f"waiting {minutes_to_wait:.1f} minutes until next expected update at {next_expected}..."
            )
            countdown_timer(int(seconds_to_wait))
        else:
            print(
                f"\n Update cycle {update_cycle}: "
                f"expected update time {next_expected} is already "
                f"{abs(seconds_to_wait) / 60:.1f} minutes in the past, checking now..."
            )

        # First attempt to fetch new data
        print(f" Update cycle {update_cycle}: Checking for new data...")
        new_df = full_run_and_plot(date, do_plot=False, output_dir=output_dir)
        new_df, _ = create_custom_ordering(new_df)
        new_max_publish = new_df["publishTime_cest"].max()

        print(f"Previous publish: {prev_max_publish}")
        print(f"New publish:      {new_max_publish}")
        print(f"Has new data:     {new_max_publish > prev_max_publish}")

        if new_max_publish > prev_max_publish:
            print(" New data found on first attempt!")
            plot_diff(prev_df, new_df, order_str, title_suffix=f"Update {update_cycle}")
            prev_df = new_df
            prev_max_publish = new_max_publish
            update_cycle += 1
            continue

        if not retry:
            print(" No new data, and retry disabled. Loop continues to next interval.")
            update_cycle += 1
            continue

        print(" No new data on first attempt. Starting retry sequence...")
        retry_found_new_data = False

        for inc in retry_increments:
            print(f" Retrying in {inc} seconds...")
            countdown_timer(inc)

            new_df = full_run_and_plot(date, do_plot=False, output_dir=output_dir)
            new_df, _ = create_custom_ordering(new_df)
            new_max_publish = new_df["publishTime_cest"].max()

            print(f" Retry check — new publish: {new_max_publish}")

            if new_max_publish > prev_max_publish:
                print(" New data found after retry!")
                plot_diff(prev_df, new_df, order_str, title_suffix=f"Update {update_cycle} (Retry)", output_dir=output_dir)
                prev_df = new_df
                prev_max_publish = new_max_publish
                retry_found_new_data = True
                break

        if not retry_found_new_data:
            print(" No new data after all retries. Waiting until next expected interval.")

        update_cycle += 1


# =========================
# CLI parsing + entry point
# =========================

def parse_args():
    parser = argparse.ArgumentParser(
        description="BMRS indicated imbalance auto-update visualiser"
    )
    parser.add_argument(
        "--date",
        default="2025-12-07",
        help="Settlement date (YYYY-MM-DD) for the BMRS day. "
             "Local C(E)ST day uses SP 47–48 from previous UTC day and 1–46 from this day.",
    )
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
    # retry defaults to True; --no-retry flips it to False
    parser.add_argument(
        "--no-retry",
        dest="retry",
        action="store_false",
        help="Disable short retry sequence between main intervals.",
    )

    parser.add_argument(
        "-o", "--output-dir",
        default=".",
        help="Directory to save output plots (default: current directory).",
    
    )
    parser.set_defaults(retry=True)

    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)
    auto_update_loop(
        date=args.date,
        update_interval_minutes=args.update_interval_minutes,
        retry=args.retry,
        retry_increments=tuple(args.retry_increments),
        output_dir=args.output_dir,
    )


if __name__ == "__main__":
    main()
