"""
python src/scanner/cli.py -y 2023 -a -d dengue zika -f csv -v

python src/scanner/cli.py -y 2010 -s SP RJ -d dengue zika -f csv parquet duckdb

python src/scanner/cli.py -y 2020 2021 2022 -s SP -d zika -f csv -o /tmp
"""

import argparse
import datetime

from src.scanner.scanner import EpiScanner
from src.scanner.utils import CACHEPATH, STATES


class YearsAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string=None):
        cur_year = datetime.datetime.now().year
        for value in values:
            if int(value) < 2010 or int(value) > cur_year:
                raise argparse.ArgumentError(
                    self,
                    f"Invalid year: {value}. " f"Options: 2010 to {cur_year}",
                )
        setattr(namespace, self.dest, list(map(lambda x: x.lower(), values)))


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


def app():
    parser = argparse.ArgumentParser(
        description="Export EpiScanner data to duckdb, csv and parquet"
    )

    parser.add_argument(
        "-y",
        "--years",
        nargs="+",
        action=StatesAction,
        help="""
            Years to be scanned.
            Example:
            -y 2011 2022
            """,
        required=True,
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
        raise parser.error(
            "one the following arguments are required: "
            "-s/--states OR -a/--all"
        )

    for year in args.years:
        for disease in args.diseases:
            for format in args.file_format:
                if args.all:
                    for state in STATES:
                        EpiScanner(
                            disease=disease,
                            uf=state,
                            year=year,
                            verbose=args.verbose,
                        ).export(to=format, output_dir=args.output_dir)
                    break

                for state in args.states:
                    EpiScanner(
                        disease=disease,
                        uf=state,
                        year=year,
                        verbose=args.verbose,
                    ).export(to=format, output_dir=args.output_dir)
