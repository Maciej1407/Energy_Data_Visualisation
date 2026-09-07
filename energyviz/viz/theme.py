"""
Plot styling, carried as data rather than module globals.

The palette imitates the FT's warm-paper house style. It lives in a frozen
dataclass so that a second theme is a value you construct, not a file you edit,
and so that every plot function takes the theme it draws with explicitly.
"""

from dataclasses import dataclass


@dataclass(frozen=True)
class Theme:
    """
    Colours and type for one plot style.

    Parameters
    ----------
    paper_bg, plot_bg : str
        Page and plotting-area backgrounds.
    grid, axis, tick : str
        Gridlines, axis lines and zero line, and all text.
    green, red : str
        The two signal colours: surplus / outturn and deficit / forecast.
    green_faded, red_faded : str
        Same signals, dimmed - used for the superseded forecast in a diff plot.
    green_tint, red_tint : str
        Very light fills, used to shade table rows.
    green_edge, red_edge : str
        Marker outlines for the emphasised series.
    font_family : str
        Any CSS font stack.
    """

    paper_bg: str = "#f2e6d8"
    plot_bg: str = "#f2e6d8"
    grid: str = "#e3d5c6"
    axis: str = "#b0977b"
    tick: str = "#6b5a4b"

    green: str = "#7bb274"
    red: str = "#c6665c"

    green_faded: str = "#b6d6b0"
    red_faded: str = "#e2a39b"

    green_tint: str = "#e3f2e1"
    red_tint: str = "#f8dad5"

    green_edge: str = "#3f6b39"
    red_edge: str = "#7c2f28"

    font_family: str = "Georgia, serif"


# The default, and the only one so far.
ft = Theme()


def apply_layout(fig, theme, title=None, **layout):
    """
    Apply the theme's page styling to a figure.

    Extra keyword arguments are passed straight through to `update_layout`, so
    a caller can add a legend title or a hover mode without repeating the
    palette.
    """
    fig.update_layout(
        title=title,
        paper_bgcolor=theme.paper_bg,
        plot_bgcolor=theme.plot_bg,
        font=dict(family=theme.font_family, color=theme.tick),
        **layout,
    )
    return fig


def apply_xaxis(fig, theme, showgrid=False, **axis):
    """Apply the theme to an x axis. Extra arguments go to `update_xaxes`."""
    fig.update_xaxes(
        showgrid=showgrid,
        gridcolor=theme.grid,
        linecolor=theme.axis,
        tickfont=dict(color=theme.tick),
        **axis,
    )
    return fig


def apply_yaxis(fig, theme, **axis):
    """
    Apply the theme to a y axis, including the emphasised zero line.

    Extra arguments go to `update_yaxes`.
    """
    fig.update_yaxes(
        gridcolor=theme.grid,
        zeroline=True,
        zerolinecolor=theme.axis,
        zerolinewidth=2,
        linecolor=theme.axis,
        tickfont=dict(color=theme.tick),
        **axis,
    )
    return fig
