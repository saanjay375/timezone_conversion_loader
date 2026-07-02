"""
config.py

Configuration models, validation and config loading.

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import json
import os
import re

from dataclasses import dataclass
from dataclasses import field

from typing import List
from typing import Optional

from datetime import datetime


###############################################################################
# EXCEPTIONS
###############################################################################

class ConfigValidationError(Exception):
    """
    Raised when configuration validation fails.
    """
    pass


###############################################################################
# DATABASE CONFIG
###############################################################################

@dataclass
class DatabaseConfig:

    host: str

    port: int

    dbname: str

    username: str

    password: str

    sslmode: Optional[str] = None

    sslrootcert: Optional[str] = None

    sslcert: Optional[str] = None

    sslkey: Optional[str] = None

    connect_timeout: int = 30

    application_name: str = (
        "timezone_conversion_loader"
    )


###############################################################################
# TABLE CONFIG
###############################################################################

@dataclass
class TableConfig:

    schema: str

    table_name: str

    driving_column: str

    chunk_size: str

    parallel_threads: int = 1

    startvalue: Optional[str] = None

    validate_rowcount: bool = False

    analyze_after_load: bool = False


###############################################################################
# GLOBAL CONFIG
###############################################################################

@dataclass
class GlobalConfig:

    database: DatabaseConfig

    source_timezone: str

    target_timezone: str = "UTC"

    newtablenamesuffix: str = "_utc"

    log_directory: str = "./logs"

    max_parallel_tables: int = 1

    max_db_connections: int = 30

    max_failed_chunks: int = 100

    statement_timeout_ms: int = (
        7200000
    )

    lock_timeout_ms: int = (
        300000
    )

    tables: List[TableConfig] = field(
        default_factory=list
    )


###############################################################################
# VALIDATION
###############################################################################

SUPPORTED_STARTVALUE_FORMATS = [

    "%d-%m-%Y",

    "%d-%m-%Y %H:%M:%S"
]


CHUNK_SIZE_PATTERN = re.compile(
    r"^[0-9]+[DdWwMmYy]$"
)


def validate_required_string(
    value,
    field_name
):

    if value is None:

        raise ConfigValidationError(
            f"{field_name} cannot be NULL"
        )

    if not str(value).strip():

        raise ConfigValidationError(
            f"{field_name} cannot be empty"
        )


def validate_startvalue(
    startvalue
):

    if startvalue is None:

        return

    for fmt in (
        SUPPORTED_STARTVALUE_FORMATS
    ):

        try:

            datetime.strptime(
                startvalue,
                fmt
            )

            return

        except ValueError:

            pass

    raise ConfigValidationError(

        f"Invalid startvalue: "
        f"{startvalue}"

        "\nSupported formats:"

        "\nDD-MM-YYYY"

        "\nDD-MM-YYYY HH:MM:SS"
    )


def validate_chunk_size(
    chunk_size
):

    if not CHUNK_SIZE_PATTERN.match(
        chunk_size
    ):

        raise ConfigValidationError(

            f"Invalid chunk size: "

            f"{chunk_size}"

            "\nExamples:"

            "\n1D"

            "\n7D"

            "\n1W"

            "\n1M"

            "\n1Y"
        )


def validate_positive_integer(
    value,
    field_name
):

    if value <= 0:

        raise ConfigValidationError(

            f"{field_name} "

            f"must be greater than zero"
        )


###############################################################################
# TABLE VALIDATOR
###############################################################################

class TableConfigValidator:

    @staticmethod
    def validate(
        table_cfg: TableConfig
    ):

        validate_required_string(
            table_cfg.schema,
            "schema"
        )

        validate_required_string(
            table_cfg.table_name,
            "table_name"
        )

        validate_required_string(
            table_cfg.driving_column,
            "driving_column"
        )

        validate_chunk_size(
            table_cfg.chunk_size
        )

        validate_positive_integer(
            table_cfg.parallel_threads,
            "parallel_threads"
        )

        validate_startvalue(
            table_cfg.startvalue
        )


###############################################################################
# GLOBAL VALIDATOR
###############################################################################

class GlobalConfigValidator:

    @staticmethod
    def validate(
        cfg: GlobalConfig
    ):

        validate_required_string(
            cfg.source_timezone,
            "source_timezone"
        )

        validate_required_string(
            cfg.newtablenamesuffix,
            "newtablenamesuffix"
        )

        validate_required_string(
            cfg.log_directory,
            "log_directory"
        )

        validate_positive_integer(
            cfg.max_parallel_tables,
            "max_parallel_tables"
        )

        validate_positive_integer(
            cfg.max_db_connections,
            "max_db_connections"
        )

        validate_positive_integer(
            cfg.statement_timeout_ms,
            "statement_timeout_ms"
        )

        validate_positive_integer(
            cfg.lock_timeout_ms,
            "lock_timeout_ms"
        )

        if not cfg.tables:

            raise ConfigValidationError(
                "No tables configured."
            )

        GlobalConfigValidator.\
            validate_database(
                cfg.database
            )

        for table_cfg in cfg.tables:

            TableConfigValidator.validate(
                table_cfg
            )

    @staticmethod
    def validate_database(
        db_cfg: DatabaseConfig
    ):

        validate_required_string(
            db_cfg.host,
            "database.host"
        )

        validate_required_string(
            db_cfg.dbname,
            "database.dbname"
        )

        validate_required_string(
            db_cfg.username,
            "database.username"
        )

        validate_required_string(
            db_cfg.password,
            "database.password"
        )

        validate_positive_integer(
            db_cfg.port,
            "database.port"
        )

        validate_positive_integer(
            db_cfg.connect_timeout,
            "connect_timeout"
        )


###############################################################################
# CONFIG LOADER
###############################################################################

class ConfigLoader:

    @staticmethod
    def load(
        config_file: str
    ) -> GlobalConfig:

        if not os.path.exists(
            config_file
        ):

            raise FileNotFoundError(
                config_file
            )

        with open(
            config_file,
            "r",
            encoding="utf-8"
        ) as handle:

            raw = json.load(handle)

        database = DatabaseConfig(

            host=raw["database"]["host"],

            port=raw["database"]["port"],

            dbname=raw["database"]["dbname"],

            username=raw["database"]["username"],

            password=raw["database"]["password"],

            sslmode=raw["database"].get(
                "sslmode"
            ),

            sslrootcert=raw["database"].get(
                "sslrootcert"
            ),

            sslcert=raw["database"].get(
                "sslcert"
            ),

            sslkey=raw["database"].get(
                "sslkey"
            ),

            connect_timeout=raw[
                "database"
            ].get(
                "connect_timeout",
                30
            ),

            application_name=raw[
                "database"
            ].get(
                "application_name",
                "timezone_conversion_loader"
            )
        )

        tables = []

        for table in raw["tables"]:

            tables.append(

                TableConfig(

                    schema=table["schema"],

                    table_name=table[
                        "table_name"
                    ],

                    driving_column=table[
                        "driving_column"
                    ],

                    chunk_size=table[
                        "chunk_size"
                    ],

                    parallel_threads=table.get(
                        "parallel_threads",
                        1
                    ),

                    startvalue=table.get(
                        "startvalue"
                    ),

                    validate_rowcount=table.get(
                        "validate_rowcount",
                        False
                    ),

                    analyze_after_load=table.get(
                        "analyze_after_load",
                        False
                    )
                )
            )

        config = GlobalConfig(

            database=database,

            source_timezone=raw[
                "source_timezone"
            ],

            target_timezone=raw.get(
                "target_timezone",
                "UTC"
            ),

            newtablenamesuffix=raw.get(
                "newtablenamesuffix",
                "_utc"
            ),

            log_directory=raw.get(
                "log_directory",
                "./logs"
            ),

            max_parallel_tables=raw.get(
                "max_parallel_tables",
                1
            ),

            max_db_connections=raw.get(
                "max_db_connections",
                30
            ),

            max_failed_chunks=raw.get(
                "max_failed_chunks",
                100
            ),

            statement_timeout_ms=raw.get(
                "statement_timeout_ms",
                7200000
            ),

            lock_timeout_ms=raw.get(
                "lock_timeout_ms",
                300000
            ),

            tables=tables
        )

        GlobalConfigValidator.validate(
            config
        )

        return config