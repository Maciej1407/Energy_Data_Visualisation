"""
Shared CLI plumbing: logging, settings, and the arguments both tasks accept.

The library logs rather than prints, so this sets up a handler that renders a
log line as a bare message - the terminal output reads the same as it always
did, but a notebook can turn it down without editing the library.
"""

import logging

from energyviz.config import Settings


def configure_logging(verbose=True):
    """Send library log lines to the terminal as plain messages."""
    logging.basicConfig(
        level=logging.INFO if verbose else logging.WARNING,
        format="%(message)s",
    )


def add_common_arguments(parser):
    """Add the arguments every task shares."""
    parser.add_argument(
        "--date",
        required=True,
        help="Settlement date (YYYY-MM-DD). The local C(E)ST day uses "
             "SP 47-48 from the previous UTC day and 1-46 from this day.",
    )
    parser.add_argument(
        "-o", "--output-dir",
        default=".",
        help="Directory to save output plots (default: current directory).",
    )
    parser.add_argument(
        "--parquet-dir",
        default=None,
        help="If given, also write the normalised data as Parquet under this directory.",
    )
    parser.add_argument(
        "--timezone",
        default=Settings.timezone,
        help=f"Local timezone for the day view (default: {Settings.timezone}).",
    )
    parser.add_argument(
        "--no-show",
        dest="show",
        action="store_false",
        help="Do not open the figures in a browser.",
    )
    parser.add_argument(
        "--quiet",
        dest="verbose",
        action="store_false",
        help="Only report warnings and errors.",
    )
    parser.set_defaults(show=True, verbose=True)
    return parser


def settings_from(args):
    """Build Settings from parsed arguments."""
    return Settings(timezone=args.timezone)
