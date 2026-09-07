"""
The only module in the package that writes files.

Everything upstream produces values - frames and figures - and this is where
they land on disk. Keeping it in one place is what stopped the save block being
copied into every plot function.
"""

import logging
import os

log = logging.getLogger(__name__)

default_width = 1600
default_height = 900
default_scale = 2


def save(fig, output_dir, base_name, formats=("png", "html"),
         width=default_width, height=default_height, scale=default_scale):
    """
    Write a figure to `output_dir/base_name.<ext>` for each requested format.

    A PNG needs a working kaleido install; if that is missing the failure is
    logged and the HTML is still written, because losing the static image is
    not a reason to lose the interactive one.

    Parameters
    ----------
    fig : plotly.graph_objects.Figure
    output_dir : str
        Created if it does not exist.
    base_name : str
        Filename without extension.
    formats : tuple of str
        Any of 'png', 'html'.
    width, height, scale : int
        PNG geometry. Ignored for HTML.

    Returns
    -------
    dict
        Format name -> path written, for the formats that succeeded.
    """
    os.makedirs(output_dir, exist_ok=True)
    base = os.path.join(output_dir, base_name)
    written = {}

    if "png" in formats:
        try:
            fig.write_image(f"{base}.png", width=width, height=height, scale=scale)
            written["png"] = f"{base}.png"
            log.info(f"Saved PNG:  {base}.png")
        except Exception as e:
            log.warning(f"FAILED TO SAVE PNG IMAGE ({base}.png): {e}")

    if "html" in formats:
        fig.write_html(f"{base}.html", include_plotlyjs="cdn")
        written["html"] = f"{base}.html"
        log.info(f"Saved HTML: {base}.html")

    return written


def height_for_table(row_count, table_fraction=0.35, cell_height=20,
                     header_height=24, padding=200):
    """
    Work out a PNG height that leaves the table readable.

    A plotly table in a fixed-height figure squashes its rows to fit, so the
    static export is sized from the row count instead of the other way round.

    Parameters
    ----------
    row_count : int
        Number of body rows in the table.
    table_fraction : float
        Share of the figure height the table subplot occupies.
    cell_height, header_height, padding : int
        Pixel allowances.
    """
    needed = header_height + row_count * cell_height
    return int(needed / table_fraction) + padding
