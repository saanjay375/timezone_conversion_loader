"""
sql_generator.py

Dynamic SQL generation layer.

Responsibilities
----------------
- Generate INSERT ... SELECT statements
- Convert timestamp columns from source timezone to UTC
- Generate range-chunk SQL
- Generate NULL-chunk SQL
- Generate dry-run preview information

Design Rules
------------
✓ INSERT ... ON CONFLICT DO NOTHING
✓ No COPY support
✓ No partition support
✓ All timestamp without time zone columns converted
✓ Non-timestamp columns copied unchanged
✓ NULL chunk processed separately

Author: Timezone Conversion Loader
"""

from __future__ import annotations

from config import GlobalConfig

from config import OperationConfig

from config import TableConfig

###############################################################################
# SQL GENERATOR
###############################################################################


class SQLGenerator:

    def __init__(
        self,
        global_config: GlobalConfig,
        operation_config: OperationConfig,
        table_config: TableConfig,
        table_context: dict,
    ):

        self.global_config = global_config
        self.operation_config = operation_config

        self.table_config = table_config

        self.table_context = table_context

    ###########################################################################
    # TARGET TABLE
    ###########################################################################

    def target_table_name(self) -> str:

        return self.table_config.table_name + self.operation_config.target_table_suffix

    ###########################################################################
    # INSERT COLUMN LIST
    ###########################################################################

    def build_insert_column_list(self) -> str:

        columns = [f'"{column}"' for column in self.table_context["all_columns"]]

        return ",".join(columns)

    ###########################################################################
    # SELECT LIST
    ###########################################################################

    def build_select_list(self) -> str:

        expressions = []

        timestamp_columns = set(self.table_context["timestamp_columns"])

        source_timezone = self.operation_config.source_timezone

        for column in self.table_context["all_columns"]:

            ###################################################################
            # TIMESTAMP COLUMN
            ###################################################################

            if column in timestamp_columns:

                #
                # Convert:
                #
                # timestamp without timezone
                #
                # FROM source_timezone
                #
                # TO UTC
                #

                expressions.append(f"""
                    (
                        "{column}"

                        AT TIME ZONE
                        '{source_timezone}'

                        AT TIME ZONE
                        'UTC'
                    )

                    AS "{column}"
                    """)

            ###################################################################
            # NORMAL COLUMN
            ###################################################################

            else:

                expressions.append(f'"{column}"')

        return ",".join(expressions)

    ###########################################################################
    # RANGE CHUNK SQL
    ###########################################################################

    def build_range_insert_sql(self) -> str:

        schema = self.table_config.schema

        source_table = self.table_config.table_name

        target_table = self.target_table_name()

        driving_column = self.table_config.driving_column

        insert_columns = self.build_insert_column_list()

        select_columns = self.build_select_list()

        return f"""
        INSERT INTO
            {schema}.{target_table}
        (
            {insert_columns}
        )

        SELECT

            {select_columns}

        FROM
            {schema}.{source_table}

        WHERE

            "{driving_column}"
                >= %s

        AND

            "{driving_column}"
                < %s

        ON CONFLICT DO NOTHING
        """

    ###########################################################################
    # NULL CHUNK SQL
    ###########################################################################

    def build_null_chunk_sql(self) -> str:

        schema = self.table_config.schema

        source_table = self.table_config.table_name

        target_table = self.target_table_name()

        driving_column = self.table_config.driving_column

        insert_columns = self.build_insert_column_list()

        select_columns = self.build_select_list()

        return f"""
        INSERT INTO
            {schema}.{target_table}
        (
            {insert_columns}
        )

        SELECT

            {select_columns}

        FROM
            {schema}.{source_table}

        WHERE

            "{driving_column}"
            IS NULL

        ON CONFLICT DO NOTHING
        """

    ###########################################################################
    # CHUNK DESCRIPTION
    ###########################################################################

    def describe_range_chunk(self, chunk) -> str:

        return f"{chunk.start_value} " f"to " f"{chunk.end_value}"

    ###########################################################################
    # PREVIEW INFO
    ###########################################################################

    def build_preview_info(self):

        schema = self.table_config.schema

        source_table = self.table_config.table_name

        target_table = self.target_table_name()

        return {
            "source_table": f"{schema}.{source_table}",
            "target_table": f"{schema}.{target_table}",
            "source_timezone": self.operation_config.source_timezone,
            "target_timezone": self.operation_config.target_timezone,
            "timestamp_columns": self.table_context["timestamp_columns"],
            "all_columns": self.table_context["all_columns"],
        }

    ###########################################################################
    # DEBUG SQL
    ###########################################################################

    def build_debug_sql(self):

        return {
            "range_sql": self.build_range_insert_sql(),
            "null_chunk_sql": self.build_null_chunk_sql(),
        }
