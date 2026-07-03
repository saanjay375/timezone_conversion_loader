"""
main.py

Timezone Conversion Loader

Entry point for:

- Configuration loading
- Validation
- Dry-run execution
- Migration execution

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import argparse
import sys
import traceback

from config import (
    ConfigLoader,
    ConfigValidationError
)

from controller import (
    MigrationController
)


###############################################################################
# ARGUMENTS
###############################################################################

def build_argument_parser():

    parser = argparse.ArgumentParser(

        prog="timezone_conversion_loader",

        description=(
            "Timezone Conversion Loader "
            "(PostgreSQL)"
        )
    )

    parser.add_argument(

        "--config",

        required=True,

        help=(
            "Configuration JSON file"
        )
    )

    parser.add_argument(

        "--dryrun",

        action="store_true",

        help=(
            "Validate configuration "
            "and estimate workload "
            "without loading data"
        )
    )

    parser.add_argument(

        "--version",

        action="version",

        version="1.0.0"
    )

    return parser


###############################################################################
# CONFIGURATION
###############################################################################

def load_configuration(
    config_file
):

    try:

        return ConfigLoader.load(
            config_file
        )

    except FileNotFoundError:

        print(

            f"ERROR: Configuration "

            f"file not found: "

            f"{config_file}"
        )

        sys.exit(1)

    except ConfigValidationError as ex:

        print(

            f"ERROR: Configuration "

            f"validation failed: "

            f"{str(ex)}"
        )

        sys.exit(1)


###############################################################################
# EXECUTION
###############################################################################

def execute_controller(
    config,
    dryrun=False
):

    controller = (
        MigrationController(
            config
        )
    )

    controller.execute(
        dryrun=dryrun
    )


###############################################################################
# MAIN
###############################################################################

def main():

    parser = build_argument_parser()

    args = parser.parse_args()

    config = load_configuration(
        args.config
    )

    print()

    print(
        "=" * 70
    )

    print(
        "Timezone Conversion Loader"
    )

    print(
        "=" * 70
    )

    print(
        f"Config File : "
        f"{args.config}"
    )

    print(
        f"Mode        : "
        f"{'DRYRUN' if args.dryrun else 'EXECUTE'}"
    )

    print()

    try:

        execute_controller(

            config,

            dryrun=args.dryrun
        )

        print()

        print(
            "=" * 70
        )

        print(
            "Execution completed."
        )

        print(
            "=" * 70
        )

        return 0

    except KeyboardInterrupt:

        print()

        print(
            "Execution cancelled "
            "by user."
        )

        return 2

    except Exception as ex:

        print()

        print(
            "=" * 70
        )

        print(
            "FATAL ERROR"
        )

        print(
            "=" * 70
        )

        print(
            str(ex)
        )

        print()

        traceback.print_exc()

        return 1


###############################################################################
# ENTRY POINT
###############################################################################

if __name__ == "__main__":

    sys.exit(
        main()
    )