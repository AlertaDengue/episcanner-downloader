"""
python src/scanner/cli.py -a -d dengue zika -f csv -v

python src/scanner/cli.py -s SP RJ DF -d dengue zika -f csv parquet duckdb

python src/scanner/cli.py -s SP -d zika -f csv -o /tmp
"""

import argparse

from src.scanner.scanner import EpiScanner
from src.scanner.utils import CACHEPATH, STATES


class StatesAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        for value in values:
            if value.upper() not in STATES:
                raise argparse.ArgumentError(self, f"Unknown UF: {value}")
        setattr(namespace, self.dest, list(map(lambda x: x.upper(), values)))


class DiseasesAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        for value in values:
            if value.lower() not in ["dengue", "zika", "chik", "chikungunya"]:
                raise argparse.ArgumentError(
                    self,
                    f"Invalid disease: {value}. "
                    "Options: dengue, zika, chik",
                )
        setattr(namespace, self.dest, list(map(lambda x: x.lower(), values)))


class OutputDataTypeAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        for value in values:
            if value.lower() not in ["csv", "parquet", "duckdb"]:
                raise argparse.ArgumentError(
                    self,
                    f"Invalid output format: {value}. "
                    "Options: csv, parquet, duckdb",
                )
        setattr(namespace, self.dest, list(map(lambda x: x.lower(), values)))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export EpiScanner data to duckdb, csv and parquet"
    )

    parser.add_argument(
        "-s",
        "--states",
        nargs="+",
        action=StatesAction,
        help="""
            The abbreviation of the state to export.
            Use --all to export data for all states.
            Example:
            -s SP RJ
            """,
        required=False,
    )

    parser.add_argument(
        "-a",
        "--all",
        action="store_true",
        default=False,
        help="Export data for all states.",
        required=False,
    )

    parser.add_argument(
        "-d",
        "--diseases",
        nargs="+",
        action=DiseasesAction,
        help="The diseases to export data for.",
        required=True,
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        type=str,
        default=CACHEPATH,
        help="The directory where the output files will be saved.",
        required=False,
    )

    parser.add_argument(
        "-f",
        "--file-format",
        nargs="+",
        action=OutputDataTypeAction,
        help="File data format. Options: csv, parquet and/or duckdb",
        default=["duckdb"],
        required=True,
    )

    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        default=False,
        help="Show verbose logs",
        required=False,
    )

    args = parser.parse_args()

    if not args.all and not bool(args.states):
        raise parser.error("No state was selected to fetch")

    for disease in args.diseases:
        for format in args.file_format:
            if args.all:
                for state in STATES:
                    EpiScanner(disease, state, args.verbose).export(
                        format, args.output_dir
                    )
                break

            for state in args.states:
                EpiScanner(disease, state, args.verbose).export(
                    format, args.output_dir
                )
