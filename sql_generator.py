"""
sql_*enerator.py

Dynamic SQL generatio*.

Author: Timezone Conversion Loa*er
"""

from __future__ import ann*tations

from config import Global*onfig
from config import TableConf*g


class SQLGenerator:

    def _*init__(
        self,
        glob*l_config: GlobalConfig,
        ta*le_config: TableConfig,
        ta*le_context: dict
    ):

        s*lf.global_config = global_config

*       self.table_config = table_c*nfig

        self.table_context =*table_context

    ###############*##################################*########################
    # TAR*ET TABLE
    #####################*##################################*##################

    def target*table_name(self):

        return *
            self.table_config.tab*e_name
            +
            s*lf.global_config.newtablenamesuffi*
        )

    ##################*##################################*#####################
    # INSERT*COLUMN LIST
    ##################*##################################*#####################

    def bui*d_insert_column_list(
        self*    ):

        return ",".join(

*           [
                f'"{c*lumn}"'

                for colum* in
                self.table_con*ext[
                    "all_colu*ns"
                ]
            *
        )

    ##################*##################################*#####################
    # SELECT*LIST
    #########################*##################################*##############

    def build_sele*t_list(
        self
    ):

     *  expressions = []

        timest*mp_columns = set(

            sel*.table_context[
                "t*mestamp_columns"
            ]
   *    )

        source_timezone = (*            self.global_config.
  *         source_timezone
        )*
        for column in (
         *  self.table_context[
            *   "all_columns"
            ]
   *    ):

            ##############*##################################*#################
            # TI*ESTAMP COLUMN
            ########*##################################*#######################

         *  if column in timestamp_columns:
*                expressions.append*

                    f'''
       *            "{column}"

          *         AT TIME ZONE
            *       '{source_timezone}'

      *             AT TIME ZONE
                    'UTC'

                    AS "{column}"
                    '''
                )

            ###################################################################
            # NORMAL COLUMN
            ###################################################################

            else:

                expressions.append(

                    f'"{column}"'
                )

        return ",".join(
            expressions
        )

    ###########################################################################
    # RANGE INSERT SQL
    ###########################################################################

    def build_range_insert_sql(
        self
    ):

        schema = (
            self.table_config.schema
        )

        source_table = (
            self.table_config.table_name
        )

        target_table = (
            self.target_table_name()
        )

        driving_column = (
            self.table_config.
            driving_column
        )

        insert_columns = (
            self.build_insert_column_list()
        )

        select_list = (
            self.build_select_list()
        )

        sql_text = f"""
        INSERT INTO
        {schema}.{target_table}
        (
            {insert_columns}
        )

        SELECT

            {select_list}

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

        return sql_text

    ###########################################################################
    # NULL CHUNK SQL
    ###########################################################################

    def build_null_chunk_sql(
        self
    ):

        schema = (
            self.table_config.schema
        )

        source_table = (
            self.table_config.table_name
        )

        target_table = (
            self.target_table_name()
        )

        driving_column = (
            self.table_config.
            driving_column
        )

        insert_columns = (
            self.build_insert_column_list()
        )

        select_list = (
            self.build_select_list()
        )

        sql_text = f"""
        INSERT INTO
        {schema}.{target_table}
        (
            {insert_columns}
        )

        SELECT

            {select_list}

        FROM
            {schema}.{source_table}

        WHERE

            "{driving_column}"
            IS NULL

        ON CONFLICT DO NOTHING
        """

        return sql_text

    ###########################################################################
    # PREDICATE DESCRIPTION
    ###########################################################################

    def describe_range_chunk(
        self,
        chunk
    ):

        return (

            f'{chunk.start_value} '

            f'to '

            f'{chunk.end_value}'
        )

    ###########################################################################
    # PREVIEW SQL (DRYRUN)
    ###########################################################################

    def build_preview_sql(
        self
    ):

        schema = (
            self.table_config.schema
        )

        source_table = (
            self.table_config.table_name
        )

        target_table = (
            self.target_table_name()
        )

        return {

            "source_table":
                f"{schema}.{source_table}",

            "target_table":
                f"{schema}.{target_table}",

            "timezone":
                (
                    self.global_config.
                    source_timezone
                )
        }