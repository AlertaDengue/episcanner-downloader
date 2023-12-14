import argparse

from utils import STATES

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Export data for a single state to Parquet and CSV files."
    )
    parser.add_argument(
        "-s",
        "--state",
        type=str,
        help="""
            The abbreviation of the state to export.
            Use 'all' to export data for all states.
            """,
        required=True,
    )
    parser.add_argument(
        "-d",
        "--diseases",
        nargs="+",
        type=str,
        help="The diseases to export data for.",
        required=True,
    )
    parser.add_argument(
        "-o",
        "--output_dir",
        type=str,
        default=None,
        help="The directory where the output files will be saved.",
        required=False,
    )

    args = parser.parse_args()

    if args.state == "all":
        for state in STATES:
            export_data_to_dir(
                state, args.diseases, output_dir=args.output_dir
            )
    else:
        export_data_to_dir(
            args.state, args.diseases, output_dir=args.output_dir
        )
