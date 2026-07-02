"""
metadata.py

Database connectivity and metadata operations.

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import psycopg2

from contextlib import contextmanager

from psycopg2 import sql

from config import GlobalConfig


###############################################################################
# EXCEPTIONS
###############################################################################

class MetadataError(Exception):
    pass


###############################################################################
# CONNECTION FACTORY
###############################################################################

class DatabaseConnectionFactory:

    def __init__(
        self,
        global_config: GlobalConfig
    ):

        self.global_config = global_config

    def build_connection_parameters(
        self
    ):

        db = self.global_config.database

        params = {

            "host": db.host,

            "port": db.port,

            "dbname": db.dbname,

            "user": db.username,

            "password": db.password,

            "connect_timeout":
                db.connect_timeout,

            "application_name":
                db.application_name
        }

        if db.sslmode:
            params["sslmode"] = db.sslmode

        if db.sslrootcert:
            params["sslrootcert"] = (
                db.sslrootcert
            )

        if db.sslcert:
            params["sslcert"] = (
                db.sslcert
            )

        if db.sslkey:
            params["sslkey"] = (
                db.sslkey
            )

        return params

    def create_connection(self):

        conn = psycopg2.connect(
            **self.build_connection_parameters()
        )

        conn.autocommit = False

        return conn


###############################################################################
# CONTEXT MANAGER
###############################################################################

@contextmanager
def get_connection(
    global_config: GlobalConfig
):

    factory = DatabaseConnectionFactory(
        global_config
    )

    conn = factory.create_connection()

    try:

        yield conn

    finally:

        conn.close()


###############################################################################
# METADATA REPOSITORY
###############################################################################

class MetadataRepository:

    def __init__(
        self,
        connection
    ):

        self.connection = connection


###############################################################################
# GENERIC HELPERS
###############################################################################

    def fetch_one(
        self,
        sql_text,
        bind_values=None
    ):

        with self.connection.cursor() as cur:

            cur.execute(
                sql_text,
                bind_values
            )

            return cur.fetchone()

    def fetch_all(
        self,
        sql_text,
        bind_values=None
    ):

        with self.connection.cursor() as cur:

            cur.execute(
                sql_text,
                bind_values
            )

            return cur.fetchall()


###############################################################################
# SESSION CONFIGURATION
###############################################################################

    def configure_session(
        self,
        statement_timeout_ms,
        lock_timeout_ms
    ):

        with self.connection.cursor() as cur:

            cur.execute(

                f"""
                SET statement_timeout =
                {statement_timeout_ms}
                """
            )

            cur.execute(

                f"""
                SET lock_timeout =
                {lock_timeout_ms}
                """
            )


###############################################################################
# TABLE VALIDATION
###############################################################################

    def table_exists(
        self,
        schema,
        table_name
    ):

        sql_text = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema=%s
        AND table_name=%s
        """

        row = self.fetch_one(

            sql_text,

            (
                schema,
                table_name
            )
        )

        return row is not None

    def validate_source_table(
        self,
        schema,
        table_name
    ):

        if not self.table_exists(
            schema,
            table_name
        ):

            raise MetadataError(

                f"Source table not found: "

                f"{schema}.{table_name}"
            )


###############################################################################
# PARTITION TABLE VALIDATION
###############################################################################

    def validate_not_partitioned(
        self,
        schema,
        table_name
    ):

        sql_text = """
        SELECT relkind
        FROM pg_class c
        JOIN pg_namespace n
          ON n.oid = c.relnamespace
        WHERE n.nspname=%s
        AND c.relname=%s
        """

        row = self.fetch_one(

            sql_text,

            (
                schema,
                table_name
            )
        )

        if row and row[0] == "p":

            raise MetadataError(

                f"Partition tables "

                f"not supported: "

                f"{schema}.{table_name}"
            )


###############################################################################
# DRIVING COLUMN VALIDATION
###############################################################################

    def validate_driving_column(
        self,
        schema,
        table_name,
        driving_column
    ):

        sql_text = """
        SELECT data_type
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s
        AND column_name=%s
        """

        row = self.fetch_one(

            sql_text,

            (
                schema,
                table_name,
                driving_column
            )
        )

        if row is None:

            raise MetadataError(

                f"Driving column not found: "

                f"{driving_column}"
            )

        data_type = row[0]

        if data_type != (
            "timestamp without time zone"
        ):

            raise MetadataError(

                f"Driving column "

                f"{driving_column} "

                f"must be timestamp "
                f"without time zone"
            )


###############################################################################
# COLUMN DISCOVERY
###############################################################################

    def get_all_columns(
        self,
        schema,
        table_name
    ):

        sql_text = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s
        ORDER BY ordinal_position
        """

        rows = self.fetch_all(

            sql_text,

            (
                schema,
                table_name
            )
        )

        return [

            row[0]

            for row in rows
        ]

    def get_timestamp_columns(
        self,
        schema,
        table_name
    ):

        sql_text = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema=%s
        AND table_name=%s
        AND data_type=
            'timestamp without time zone'
        ORDER BY ordinal_position
        """

        rows = self.fetch_all(

            sql_text,

            (
                schema,
                table_name
            )
        )

        return [

            row[0]

            for row in rows
        ]


###############################################################################
# MIN/MAX VALUES
###############################################################################

    def get_min_value(
        self,
        schema,
        table_name,
        driving_column
    ):

        query = sql.SQL(

            """
            SELECT MIN({})
            FROM {}.{}
            """

        ).format(

            sql.Identifier(
                driving_column
            ),

            sql.Identifier(
                schema
            ),

            sql.Identifier(
                table_name
            )
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]

    def get_max_value(
        self,
        schema,
        table_name,
        driving_column
    ):

        query = sql.SQL(

            """
            SELECT MAX({})
            FROM {}.{}
            """

        ).format(

            sql.Identifier(
                driving_column
            ),

            sql.Identifier(
                schema
            ),

            sql.Identifier(
                table_name
            )
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]


###############################################################################
# ROW COUNTS
###############################################################################

    def table_rowcount(
        self,
        schema,
        table_name
    ):

        query = sql.SQL(

            """
            SELECT COUNT(*)
            FROM {}.{}
            """

        ).format(

            sql.Identifier(schema),

            sql.Identifier(table_name)
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

            return cur.fetchone()[0]


###############################################################################
# INDEX VALIDATION
###############################################################################

    def driving_column_index_exists(
        self,
        schema,
        table_name,
        driving_column
    ):

        sql_text = """
        SELECT
            indexname,
            indexdef
        FROM pg_indexes
        WHERE schemaname=%s
        AND tablename=%s
        """

        rows = self.fetch_all(

            sql_text,

            (
                schema,
                table_name
            )
        )

        for idx_name, idx_def in rows:

            if driving_column in idx_def:

                return (

                    True,

                    idx_name
                )

        return (

            False,

            None
        )


###############################################################################
# TARGET TABLE CREATION
###############################################################################

    def create_target_table_if_needed(
        self,
        schema,
        source_table,
        target_table
    ):

        if self.table_exists(
            schema,
            target_table
        ):

            return

        ddl = sql.SQL(

            """
            CREATE TABLE {}.{}
            (
                LIKE {}.{}
                INCLUDING DEFAULTS
                INCLUDING CONSTRAINTS
                INCLUDING STORAGE
            )
            """

        ).format(

            sql.Identifier(schema),

            sql.Identifier(target_table),

            sql.Identifier(schema),

            sql.Identifier(source_table)
        )

        with self.connection.cursor() as cur:

            cur.execute(ddl)

        self.connection.commit()


###############################################################################
# ANALYZE SUPPORT
###############################################################################

    def analyze_table(
        self,
        schema,
        table_name
    ):

        query = sql.SQL(

            """
            ANALYZE {}.{}
            """

        ).format(

            sql.Identifier(schema),

            sql.Identifier(table_name)
        )

        with self.connection.cursor() as cur:

            cur.execute(query)

        self.connection.commit()


###############################################################################
# TABLE SIZE
###############################################################################

    def get_table_size_gb(
        self,
        schema,
        table_name
    ):

        row = self.fetch_one(

            """
            SELECT
                pg_total_relation_size(%s)
            """,

            (
                f"{schema}.{table_name}",
            )
        )

        bytes_value = row[0]

        return round(

            bytes_value
            / 1024
            / 1024
            / 1024,

            2
        )