"""
sql_generator.py

Dynamic SQL generation layer.

Author: Timezone Conversion Loader
"""

from __future__ import annotations

from config import GlobalConfig, OperationConfig, TableConfig


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

    def target_table_name(self) -> str:
        return self.table_config.table_name + self.operation_config.target_table_suffix

    def build_insert_column_list(self) -> str:
        return ",".join(
            f'"{column}"' for column in self.table_context["all_columns"]
        )

    def build_select_list(self) -> str:
        expressions = []
        timestamp_columns = set(self.table_context["timestamp_columns"])
        source_timezone = self.operation_config.source_timezone
        target_timezone = self.operation_config.target_timezone

        for column in self.table_context["all_columns"]:
            if column in timestamp_columns:
                expressions.append(
                    f'''(
                        "{column}"
                        AT TIME ZONE '{source_timezone}'
                        AT TIME ZONE '{target_timezone}'
                    ) AS "{column}"'''
                )
            else:
                expressions.append(f'"{column}"')

        return ",".join(expressions)

    def build_range_insert_sql(self) -> str:
        schema = self.table_config.schema
        source_table = self.table_config.table_name
        target_table = self.target_table_name()
        driving_column = self.table_config.driving_column

        return f'''
        INSERT INTO {schema}.{target_table}
        (
            {self.build_insert_column_list()}
        )
        SELECT
            {self.build_select_list()}
        FROM {schema}.{source_table}
        WHERE "{driving_column}" >= %s
          AND "{driving_column}" < %s
        ON CONFLICT DO NOTHING
        '''

    def build_null_chunk_sql(self) -> str:
        schema = self.table_config.schema
        source_table = self.table_config.table_name
        target_table = self.target_table_name()
        driving_column = self.table_config.driving_column

        return f'''
        INSERT INTO {schema}.{target_table}
        (
            {self.build_insert_column_list()}
        )
        SELECT
            {self.build_select_list()}
        FROM {schema}.{source_table}
        WHERE "{driving_column}" IS NULL
        ON CONFLICT DO NOTHING
        '''

    def _build_primary_key_join(self) -> str:
        primary_key_columns = self.table_context.get("primary_key_columns", [])

        if not primary_key_columns:
            raise ValueError(
                "Timestamp update validation requires a primary key on the source table"
            )

        return " AND ".join(
            f's."{column}" = t."{column}"'
            for column in primary_key_columns
        )

    def _build_timestamp_mismatch_expression(self) -> str:
        timestamp_columns = self.table_context["timestamp_columns"]
        source_timezone = self.operation_config.source_timezone
        target_timezone = self.operation_config.target_timezone
        primary_key_columns = self.table_context.get("primary_key_columns", [])

        if not primary_key_columns:
            raise ValueError(
                "Timestamp update validation requires a primary key on the source table"
            )

        target_missing = f't."{primary_key_columns[0]}" IS NULL'

        column_mismatches = [
            f'''t."{column}" IS DISTINCT FROM
                (
                    s."{column}"
                    AT TIME ZONE '{source_timezone}'
                    AT TIME ZONE '{target_timezone}'
                )'''
            for column in timestamp_columns
        ]

        if not column_mismatches:
            return target_missing

        return " OR ".join([target_missing, *column_mismatches])

    def _build_timestamp_validation_sql(self, where_clause: str) -> str:
        schema = self.table_config.schema
        source_table = self.table_config.table_name
        target_table = self.target_table_name()
        join_predicate = self._build_primary_key_join()
        mismatch_expression = self._build_timestamp_mismatch_expression()

        return f'''
        SELECT
            COUNT(*) AS rows_validated,
            COUNT(*) FILTER (
                WHERE {mismatch_expression}
            ) AS mismatch_count
        FROM {schema}.{source_table} s
        LEFT JOIN {schema}.{target_table} t
          ON {join_predicate}
        WHERE {where_clause}
        '''

    def build_range_timestamp_validation_sql(self) -> str:
        driving_column = self.table_config.driving_column
        return self._build_timestamp_validation_sql(
            f's."{driving_column}" >= %s AND s."{driving_column}" < %s'
        )

    def build_null_timestamp_validation_sql(self) -> str:
        driving_column = self.table_config.driving_column
        return self._build_timestamp_validation_sql(
            f's."{driving_column}" IS NULL'
        )

    def describe_range_chunk(self, chunk) -> str:
        return f"{chunk.start_value} to {chunk.end_value}"

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

    def build_debug_sql(self):
        return {
            "range_sql": self.build_range_insert_sql(),
            "null_chunk_sql": self.build_null_chunk_sql(),
            "range_timestamp_validation_sql": (
                self.build_range_timestamp_validation_sql()
            ),
            "null_timestamp_validation_sql": (
                self.build_null_timestamp_validation_sql()
            ),
        }
