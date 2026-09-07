"""
Figure builders: normalised frame in, plotly Figure out.

Nothing here touches the network, the filesystem, or the clock, and nothing
here calls `fig.show()`. A figure is a value; deciding to display or write it
belongs to the caller. That split is what makes these testable without a
browser and reusable from a notebook.
"""

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..domain import settlement
from . import theme as theming


# =========================
# Titles
# =========================

def snapshot_title(df, timezone):
    """
    Title for a single imbalance snapshot: settlement date and publish time.
    """
    main_date = pd.to_datetime(df["settlement_date"]).max()
    published = settlement.to_local(df["publish_utc"], timezone).max()

    date_str = main_date.strftime("%d %b %Y")
    time_str = published.strftime("%H:%M %Z")

    return f"Indicated Imbalance per Settlement Period — {date_str}, {time_str}"


def diff_title(previous, latest, same_date, timezone, suffix=""):
    """
    Title for a forecast-evolution plot, naming both publish times.

    When the two snapshots share a settlement date the date is stated once;
    otherwise each side carries its own.
    """
    previous_publish = settlement.to_local(previous["publish_utc"], timezone).max()
    latest_publish = settlement.to_local(latest["publish_utc"], timezone).max()

    previous_time = previous_publish.strftime("%H:%M %Z")
    latest_time = latest_publish.strftime("%H:%M %Z")

    if same_date:
        date_str = pd.to_datetime(previous["settlement_date"]).max().strftime("%d %b %Y")
        title = (
            f"Imbalance per Settlement Period {date_str}: "
            f"{previous_time} vs {latest_time}"
        )
    else:
        previous_date = pd.to_datetime(previous["settlement_date"]).max().strftime("%d %b %Y")
        latest_date = pd.to_datetime(latest["settlement_date"]).max().strftime("%d %b %Y")
        title = (
            f"Imbalance per Settlement Period "
            f"{previous_date} {previous_time} vs {latest_date} {latest_time}"
        )

    return f"{title} ({suffix})" if suffix else title


def forecast_vs_actual_title(df, label):
    """Title for a forecast-vs-actual plot, dated from the local start time."""
    local = df["start_local"].iloc[0]
    return (
        f"{label} generation — forecast vs actual — "
        f"{local.strftime('%d %b %Y')} ({local.strftime('%Z')})"
    )


# =========================
# Imbalance - single snapshot
# =========================

def imbalance_snapshot(df, timezone, theme=theming.ft, title=None):
    """
    Scatter the live imbalance forecast, one point per settlement period.

    Parameters
    ----------
    df : pandas.DataFrame
        Output of `domain.imbalance.latest_per_period` with `add_sign` applied.
    timezone : str
        Used only to render the publish time in the title.
    theme : Theme
    title : str, optional
        Overrides the generated title.
    """
    df = df.copy()
    title = title if title is not None else snapshot_title(df, timezone)

    fig = px.scatter(
        df,
        x="settlement_period_str",
        y="value",
        title=title,
        category_orders={"settlement_period_str": settlement.period_order()},
        color="value_sign",
        color_discrete_map={"Positive": theme.green, "Negative": theme.red},
        labels={
            "settlement_period_str": "Settlement Period",
            "value": "Indicated Imbalance (MW)",
            "value_sign": "Imbalance Sign",
        },
    )

    fig.update_traces(marker=dict(size=8, opacity=0.9, line=dict(width=0)))

    theming.apply_layout(fig, theme, title=title)
    theming.apply_xaxis(fig, theme, showgrid=False)
    theming.apply_yaxis(fig, theme)

    return fig


# =========================
# Imbalance - forecast evolution
# =========================

def imbalance_diff(merged, theme=theming.ft, title=None):
    """
    Show how each settlement period moved between two forecast snapshots.

    The superseded value is drawn faded, the live one bold, and a dotted stem
    joins them so the direction of the revision is readable at a glance.

    Parameters
    ----------
    merged : pandas.DataFrame
        First element of `domain.imbalance.diff_snapshots`.
    theme : Theme
    title : str, optional
    """
    merged = merged.copy()

    previous_mask = merged["value_prev"].notna()
    latest_mask = merged["value_new"].notna()

    fig = go.Figure()

    # Superseded points, faded.
    _add_sign_traces(
        fig, merged, previous_mask,
        value_column="value_prev",
        sign_column="value_prev_sign",
        name_prefix="Previous",
        hover_name="Previous",
        colours={"Positive": theme.green_faded, "Negative": theme.red_faded},
        marker=dict(size=8, opacity=0.7),
    )

    # Live points, bold and outlined.
    _add_sign_traces(
        fig, merged, latest_mask,
        value_column="value_new",
        sign_column="value_new_sign",
        name_prefix="Latest",
        hover_name="Latest",
        colours={"Positive": theme.green, "Negative": theme.red},
        marker=dict(size=12, opacity=0.95),
        edges={"Positive": theme.green_edge, "Negative": theme.red_edge},
    )

    # Stems joining the two, coloured by direction of the revision.
    both = merged[previous_mask & latest_mask]
    for _, row in both.iterrows():
        fig.add_trace(go.Scatter(
            x=[row["settlement_period_str"], row["settlement_period_str"]],
            y=[row["value_prev"], row["value_new"]],
            mode="lines",
            line=dict(
                color=theme.green if row["delta"] > 0 else theme.red,
                width=2,
                dash="dot",
            ),
            showlegend=False,
            hoverinfo="skip",
        ))

    theming.apply_layout(
        fig, theme,
        title=title,
        hovermode="closest",
        legend_title_text="Forecast version",
    )
    theming.apply_xaxis(
        fig, theme,
        showgrid=True,
        gridwidth=1,
        title="Settlement Period",
        categoryorder="array",
        categoryarray=settlement.period_order(),
    )
    theming.apply_yaxis(fig, theme, title="Indicated Imbalance (MW)", showgrid=True, gridwidth=1)

    return fig


def _add_sign_traces(fig, merged, mask, value_column, sign_column,
                     name_prefix, hover_name, colours, marker, edges=None):
    """
    Add one trace per sign, skipping signs with nothing to draw.

    Split by sign rather than coloured per point so that the legend carries four
    meaningful entries instead of a colour bar.
    """
    for sign, colour in colours.items():
        selected = mask & (merged[sign_column] == sign)
        if not selected.any():
            continue

        style = dict(marker, color=colour)
        if edges is not None:
            style["line"] = dict(width=1, color=edges[sign])

        fig.add_trace(go.Scatter(
            x=merged.loc[selected, "settlement_period_str"],
            y=merged.loc[selected, value_column],
            mode="markers",
            name=f"{name_prefix} ({sign})",
            marker=style,
            showlegend=True,
            hovertemplate=(
                "Settlement Period: %{x}<br>"
                "Indicated Imbalance (MW): %{y}"
                f"<extra>{hover_name}</extra>"
            ),
        ))


# =========================
# Wind and solar - forecast vs actual
# =========================

def forecast_vs_actual(df, label, theme=theming.ft, x_axis="settlement_period", title=None):
    """
    Forecast against outturn, over a table of the same numbers.

    Parameters
    ----------
    df : pandas.DataFrame
        Output of `domain.windsolar.split` for one series.
    label : str
        'Wind' or 'Solar', used in the title and the trace names.
    theme : Theme
    x_axis : str
        'settlement_period' or 'start_local'.
    title : str, optional
    """
    if df.empty:
        raise ValueError(f"{label}: no data to plot.")

    if x_axis not in ("settlement_period", "start_local"):
        raise ValueError("x_axis must be 'settlement_period' or 'start_local'")

    df = df.copy()
    title = title if title is not None else forecast_vs_actual_title(df, label)

    if x_axis == "settlement_period":
        df = settlement.add_period_labels(df)
        x_values = df["settlement_period_str"]
        x_title = "Settlement Period"
        category_args = dict(categoryorder="array", categoryarray=settlement.period_order())
    else:
        df = df.sort_values("start_local").reset_index(drop=True)
        x_values = df["start_local"]
        x_title = "Local start time"
        category_args = {}

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        row_heights=[0.65, 0.35],
        specs=[[{"type": "scatter"}], [{"type": "table"}]],
    )

    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=df["forecast_mw"],
            mode="lines+markers",
            name=f"{label} forecast",
            marker=dict(size=7, opacity=0.9, line=dict(width=0)),
            line=dict(width=2, color=theme.red),
            hovertemplate=f"{x_axis}: %{{x}}<br>Forecast: %{{y:.1f}} MW<extra></extra>",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=x_values,
            y=df["actual_mw"],
            mode="lines+markers",
            name=f"{label} actual",
            marker=dict(size=7, opacity=0.9, line=dict(width=0)),
            line=dict(width=2, dash="dot", color=theme.green),
            hovertemplate=f"{x_axis}: %{{x}}<br>Actual: %{{y:.1f}} MW<extra></extra>",
        ),
        row=1, col=1,
    )

    fig.add_trace(_error_table(df, theme), row=2, col=1)

    theming.apply_layout(
        fig, theme,
        title=title,
        legend_title_text="Series",
        hovermode="x unified" if x_axis == "settlement_period" else "closest",
        margin=dict(t=60, b=40),
    )
    theming.apply_yaxis(fig, theme, title_text="Generation (MW)", row=1, col=1)
    theming.apply_xaxis(fig, theme, row=1, col=1, **category_args)
    theming.apply_xaxis(fig, theme, row=2, col=1, title_text=x_title, **category_args)

    return fig


def _error_table(df, theme):
    """
    Build the numbers table, each row tinted by the sign of the forecast error.
    """
    table = df[["settlement_period", "forecast_mw", "actual_mw", "diff_mw"]].round(1)

    row_colours = [
        theme.plot_bg if pd.isna(v) else theme.green_tint if v >= 0 else theme.red_tint
        for v in table["diff_mw"]
    ]

    return go.Table(
        header=dict(
            values=["SP", "Forecast (MW)", "Actual (MW)", "Actual - Forecast (MW)"],
            align="center",
            font=dict(size=12, color=theme.paper_bg),
            fill_color=theme.axis,
        ),
        cells=dict(
            values=[table[col] for col in table.columns],
            align="center",
            fill_color=[row_colours] * len(table.columns),
            font=dict(color=theme.tick),
        ),
        columnwidth=[0.8, 1.4, 1.4, 1.6],
    )
