"""
Task 1 entry point - kept so that the original command line still works.

The code moved into the energyviz package; this delegates to cli/imbalance.py
and only supplies the settlement date default the old script had.

    python task1.py --date 2025-12-07 -o out_task1
"""

import sys

from cli import imbalance

default_date = "2025-12-07"


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    if not any(arg == "--date" or arg.startswith("--date=") for arg in argv):
        argv = ["--date", default_date] + argv

    return imbalance.main(argv)


if __name__ == "__main__":
    sys.exit(main())
