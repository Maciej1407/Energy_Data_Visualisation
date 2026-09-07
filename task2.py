"""
Task 2 entry point - kept so that the original command line still works.

The code moved into the energyviz package; this delegates to cli/windsolar.py
and only supplies the settlement date default the old script had.

    python task2.py --date 2025-11-11 --x-axis startTime_cest -o out_task2
"""

import sys

from cli import windsolar

default_date = "2025-11-11"


def main(argv=None):
    argv = list(sys.argv[1:] if argv is None else argv)

    if not any(arg == "--date" or arg.startswith("--date=") for arg in argv):
        argv = ["--date", default_date] + argv

    return windsolar.main(argv)


if __name__ == "__main__":
    sys.exit(main())
