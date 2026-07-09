"""
metadata.py

PostgreSQL connectivity and metadata repository.

Responsibilities
----------------
- Database connections
- Session configuration
- Source table validation
- Partition validation
- Driving-column validation
- Timestamp column discovery
- Min/Max retrieval
- Rowcount validation support
- Target-table creation
- Index validation
- ANALYZE support

Author: Timezone Conversion Loader
"""

from __future__ import annotations

from contextlib import contextmanager
import re

import psycopg2

from psycopg2 import sql


from config import (
    GlobalConfig,
    OperationConfig,
    get_effective_postgresql_session_settings,
)

###############################################################################
# EXCEPTIONS
###############################################################################


class MetadataError(Exception):
    """Metadata related exception."""

    pass


###############################################################################
# POSTGRESQL SESSION SETTINGS
###############################################################################


POSTGRESQL_SETTING_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_.]*$")


def validate_postgresql_session_setting_name(
    setting_name: str,
):

    if not POSTGRESQL_SETTING_NAME_PATTERN.match(setting_name):

        raise ValueError(f"Invalid PostgreSQL session setting name: " f"{setting_name}")


def apply_postgresql_session_settings(
    conn,
    settings: dict[str, object],
):

    if not settings:

        return

    with conn.cursor() as cur:

        for setting_name, setting_value in settings.items():

            validate_postgresql_session_setting_name(setting_name)

            cur.execute(
                f"SET {setting_name} = %s",
                (str(setting_value),),
            )

    conn.commit()


###############################################################################
# CONNECTION FACTORY
###############################################################################


class DatabaseConnectionFactory:

    def __init__(self, global_config: GlobalConfig):
        self.global_config = global_config

    def build_connection_parameters(self):

        db = self.global_config.database

        params = {
            "host": db.host,
            "port": db.port,
            "dbname": db.dbname,
            "user": db.username,
            "password": db.password,
            "connect_timeout": db.connect_timeout,
            "application_name": db.application_name,
        }

        if db.sslmode:
            params["sslmode"] = db.sslmode

        if db.sslrootcert:
            params["sslrootcert"] = db.sslrootcert

        if db.sslcert:
            params["sslcert"] = db.sslcert

        if db.sslkey:
            params["sslkey"] = db.sslkey

        return params

    def create_connection(self):

        conn = psycopg2.connect(**self.build_connection_parameters())

        conn.autocommit = False

        return conn


###############################################################################
# CONNECTION CONTEXT
###############################################################################


@contextmanager
def get_connection(
    global_config: GlobalConfig,
    operation_config: OperationConfig | None = None,
):

    factory = DatabaseConnectionFactory(global_config)

    conn = factory.create_connection()

    effective_settings = (
        get_effective_postgresql_session_settings(
            global_config,
            operation_config,
        )
        if operation_config
        else dict(global_config.postgresql_session_settings)
    )

    apply_postgresql_session_settings(
        conn,
        effective_settings,
    )

    try:

        yield conn

    finally:

        conn.close()


###############################################################################
# METADATA REPOSITORY
###############################################################################


class MetadataRepository:

    def __init__(self, connection):
        self.connection = connection

    ###########################################################################
    # GENERIC HELPERS
    ###########################################################################

    def fetch_one(self, sql_text, bind_values=None):

        with self.connection.cursor() as cur:

            cur.execute(sql_text, bind_values)

            return cur.fetchone()

    def fetch_all(self, sql_text, bind_values=None):

        with self.connection.cursor() as cur:

            cur.execute(sql_text, bind_values)

            return cur.fetchall()

    ###########################################################################
    # SESSION CONFIGURATION
    ###########################################################################

    def configure_session(self, statement_timeout_ms, lock_timeout_ms):

        with self.connection.cursor() as cur:

            cur.execute(f"""
                SET statement_timeout =
                {statement_timeout_ms}
                """)

            cur.execute(f"""
                SET lock_timeout =
                {lock_timeout_ms}
                """)

    ###########################################################################
    # TABLE EXISTS
    ###########################################################################

    def table_exists(self, schema, table_name):

        row = self.fetch_one(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_name = %s
            """,
            (schema, table_name),
        )

        return row is not None

    ###########################################################################
    # SOURCE TABLE VALIDATION
    ###########################################################################

    def validate_source_table(self, schema, table_name):

        if not self.table_exists(schema, table_name):

            raise MetadataError(f"Source table not found: " f"{schema}.{table_name}")

    ###########################################################################
    # PARTITION VALIDATION
    ###########################################################################

    def validate_not_partitioned(self, schema, table_name):

        row = self.fetch_one(
            """
            SELECT c.relkind
            FROM pg_class c
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
            """,
            (schema, table_name),
        )

        if row and row[0] == "p":

            raise MetadataError(
                f"Partition tables " f"not supported: " f"{schema}.{table_name}"
            )

    ###########################################################################
    # DRIVING COLUMN VALIDATION
    ###########################################################################

    def validate_driving_column(self, schema, table_name, driving_column):

        row = self.fetch_one(
            """
            SELECT data_type
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND column_name = %s
            """,
            (schema, table_name, driving_column),
        )

        if row is None:

            raise MetadataError(f"Driving column not found: " f"{driving_column}")

        data_type = row[0]

        if data_type != ("timestamp without time zone"):

            raise MetadataError(
                f"Driving column "
                f"{driving_column} "
                f"must be timestamp "
                f"without time zone"
            )

    ###########################################################################
    # COLUMN DISCOVERY
    ###########################################################################

    def get_all_columns(self, schema, table_name):

        rows = self.fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table_name),
        )

        return [row[0] for row in rows]

    def get_timestamp_columns(self, schema, table_name):

        rows = self.fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
              AND data_type =
                  'timestamp without time zone'
            ORDER BY ordinal_position
            """,
            (schema, table_name),
        )

        return [row[0] for row in rows]

    ###########################################################################
    # MIN / MAX
    ###########################################################################

    def get_min_value(self, schema, table_name, driving_column):

        query = sql.SQL("""
            SELECT MIN({})
            FROM {}.{}
            """).format(
            sql.Identifier(driving_column),
            sql.Identifier(schema),
            sql.Identifier(table_name),
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]

    def get_max_value(self, schema, table_name, driving_column):

        query = sql.SQL("""
            SELECT MAX({})
            FROM {}.{}
            """).format(
            sql.Identifier(driving_column),
            sql.Identifier(schema),
            sql.Identifier(table_name),
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]

    ###########################################################################
    # ROWCOUNT
    ###########################################################################

    def table_rowcount(self, schema, table_name):

        query = sql.SQL("""
            SELECT COUNT(*)
            FROM {}.{}
            """).format(sql.Identifier(schema), sql.Identifier(table_name))

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]

    ###########################################################################
    # INDEX VALIDATION
    ###########################################################################

    def driving_column_index_exists(self, schema, table_name, driving_column):

        rows = self.fetch_all(
            """
            SELECT
                indexname,
                indexdef
            FROM pg_indexes
            WHERE schemaname = %s
              AND tablename = %s
            """,
            (schema, table_name),
        )

        for index_name, index_def in rows:

            if driving_column in index_def:

                return (True, index_name)

        return (False, None)

    ###########################################################################
    # TABLE SIZE
    ###########################################################################

    def get_table_size_gb(self, schema, table_name):

        row = self.fetch_one(
            """
            SELECT pg_total_relation_size(%s)
            """,
            (f"{schema}.{table_name}",),
        )

        if row is None:

            return 0.0

        size_bytes = row[0]

        return round(size_bytes / 1024 / 1024 / 1024, 2)

    ###########################################################################
    # TARGET TABLE CREATION
    ###########################################################################

    def get_primary_key_columns(self, schema, table_name):
        rows = self.fetch_all(
            """
            SELECT a.attname
            FROM pg_index i
            JOIN pg_class t
                ON t.oid = i.indrelid
            JOIN pg_namespace n
                ON n.oid = t.relnamespace
            JOIN pg_attribute a
                ON a.attrelid = t.oid
               AND a.attnum = ANY(i.indkey)
            WHERE n.nspname = %s
              AND t.relname = %s
              AND i.indisprimary = true
            ORDER BY a.attnum
            """,
            (schema, table_name),
        )

        return [row[0] for row in rows]

    def create_primary_key(self, schema, target_table, pk_columns):
        if not pk_columns:
            return

        constraint_name = f"{target_table}_pk"

        columns = sql.SQL(", ").join([sql.Identifier(col) for col in pk_columns])

        stmt = sql.SQL("""
            ALTER TABLE {}.{}
            ADD CONSTRAINT {}
            PRIMARY KEY ({})
            """).format(
            sql.Identifier(schema),
            sql.Identifier(target_table),
            sql.Identifier(constraint_name),
            columns,
        )

        with self.connection.cursor() as cur:
            cur.execute(stmt)

        self.connection.commit()

    def create_target_table_if_needed(self, schema, source_table, target_table):
        if self.table_exists(schema, target_table):
            return

        ddl = sql.SQL("""
            CREATE TABLE {}.{}
            (
                LIKE {}.{}
                INCLUDING DEFAULTS
                INCLUDING STORAGE
            )
            """).format(
            sql.Identifier(schema),
            sql.Identifier(target_table),
            sql.Identifier(schema),
            sql.Identifier(source_table),
        )

        with self.connection.cursor() as cur:
            cur.execute(ddl)

        self.connection.commit()

        pk_columns = self.get_primary_key_columns(schema, source_table)

        self.create_primary_key(schema, target_table, pk_columns)

    ###########################################################################
    # ANALYZE
    ###########################################################################

    def analyze_table(self, schema, table_name):

        stmt = sql.SQL("""
            ANALYZE {}.{}
            """).format(sql.Identifier(schema), sql.Identifier(table_name))

        with self.connection.cursor() as cur:

            cur.execute(stmt)

        self.connection.commit()

    ###########################################################################
    # TARGET TABLE EXISTS
    ###########################################################################

    def validate_target_table(self, schema, table_name):

        if not self.table_exists(schema, table_name):

            raise MetadataError(f"Target table not found: " f"{schema}.{table_name}")

    ###########################################################################
    # UNIQUE / PK VALIDATION
    ###########################################################################

    def unique_or_pk_exists(self, schema, table_name):

        row = self.fetch_one(
            """
            SELECT COUNT(*)
            FROM pg_constraint c
            JOIN pg_class t
              ON t.oid = c.conrelid
            JOIN pg_namespace n
              ON n.oid = t.relnamespace
            WHERE n.nspname = %s
              AND t.relname = %s
              AND c.contype IN ('p','u')
            """,
            (schema, table_name),
        )

        return row is not None and row[0] > 0
