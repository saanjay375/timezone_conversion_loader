"""
chunking.py

Chunk generation utilities.

Responsibilities
----------------
- Chunk object
- StartValue parsing
- Chunk size parsing
- Date arithmetic
- Chunk generation
- NULL chunk generation
- Queue creation

Author: Timezone Conversion Loader
"""

from __future__ import annotations

import queue

from dataclasses import dataclass

from datetime import datetime
from datetime import timedelta

from calendar import monthrange


###############################################################################
# EXCEPTIONS
###############################################################################

class ChunkGenerationError(Exception):
    pass


###############################################################################
# CHUNK MODEL
###############################################################################

@dataclass
class Chunk:

    chunk_number: int

    predicate: str

    start_value: datetime | None

    end_value: datetime | None

    is_null_chunk: bool = False


###############################################################################
# STARTVALUE SUPPORT
###############################################################################

SUPPORTED_STARTVALUE_FORMATS = [

    "%d-%m-%Y",

    "%d-%m-%Y %H:%M:%S"
]


def convert_startvalue(
    value: str
) -> datetime:

    for fmt in SUPPORTED_STARTVALUE_FORMATS:

        try:

            return datetime.strptime(
                value,
                fmt
            )

        except ValueError:

            pass

    raise ChunkGenerationError(
        f"Invalid startvalue: {value}"
    )


###############################################################################
# CHUNK SIZE PARSER
###############################################################################

def parse_chunk_size(
    chunk_size: str
):

    chunk_size = chunk_size.strip()

    unit = chunk_size[-1].upper()

    value = int(
        chunk_size[:-1]
    )

    if unit not in (
        "D",
        "W",
        "M",
        "Y"
    ):

        raise ChunkGenerationError(
            f"Unsupported chunk size: {chunk_size}"
        )

    return value, unit


###############################################################################
# DATE HELPERS
###############################################################################

def add_months(
    source_date: datetime,
    months: int
):

    month = (
        source_date.month
        - 1
        + months
    )

    year = (
        source_date.year
        + month // 12
    )

    month = (
        month % 12
    ) + 1

    day = min(
        source_date.day,
        monthrange(
            year,
            month
        )[1]
    )

    return source_date.replace(
        year=year,
        month=month,
        day=day
    )


def add_years(
    source_date: datetime,
    years: int
):

    try:

        return source_date.replace(
            year=
            source_date.year
            + years
        )

    except ValueError:

        return source_date.replace(
            month=2,
            day=28,
            year=
            source_date.year
            + years
        )


###############################################################################
# NEXT BOUNDARY
###############################################################################

def compute_next_boundary(
    current_date,
    chunk_value,
    chunk_unit
):

    if chunk_unit == "D":

        return (
            current_date
            +
            timedelta(days=chunk_value)
        )

    if chunk_unit == "W":

        return (
            current_date
            +
            timedelta(weeks=chunk_value)
        )

    if chunk_unit == "M":

        return add_months(
            current_date,
            chunk_value
        )

    if chunk_unit == "Y":

        return add_years(
            current_date,
            chunk_value
        )

    raise ChunkGenerationError(
        f"Unsupported unit: {chunk_unit}"
    )


###############################################################################
# PREDICATE
###############################################################################

def build_chunk_predicate(
    start_value,
    end_value
):

    return (

        f">={start_value} "

        f"<{end_value}"
    )


###############################################################################
# GENERATE CHUNKS
###############################################################################

def generate_chunks(
    start_value,
    max_value,
    chunk_size,
    driving_column
):

    if start_value is None:

        raise ChunkGenerationError(
            "start_value cannot be NULL"
        )

    if max_value is None:

        raise ChunkGenerationError(
            "max_value cannot be NULL"
        )

    chunk_value, chunk_unit = (
        parse_chunk_size(
            chunk_size
        )
    )

    chunks = []

    chunk_number = 1

    ###########################################################################
    # SPECIAL CASE
    #
    # MIN == MAX
    ###########################################################################

    if start_value == max_value:

        chunks.append(

            Chunk(

                chunk_number=
                    chunk_number,

                predicate=
                    f"={start_value}",

                start_value=
                    start_value,

                end_value=
                    (
                        max_value
                        +
                        timedelta(
                            microseconds=1
                        )
                    ),

                is_null_chunk=False
            )
        )

        chunk_number += 1

    ###########################################################################
    # NORMAL CHUNK GENERATION
    ###########################################################################

    else:

        current = start_value

        while current <= max_value:

            next_boundary = (
                compute_next_boundary(

                    current,

                    chunk_value,

                    chunk_unit
                )
            )

            ###################################################################
            # FINAL CHUNK
            #
            # Include MAX(timestamp)
            ###################################################################

            if next_boundary > max_value:

                next_boundary = (

                    max_value

                    +

                    timedelta(
                        microseconds=1
                    )
                )

            chunks.append(

                Chunk(

                    chunk_number=
                        chunk_number,

                    predicate=
                        build_chunk_predicate(

                            current,

                            next_boundary
                        ),

                    start_value=
                        current,

                    end_value=
                        next_boundary,

                    is_null_chunk=
                        False
                )
            )

            chunk_number += 1

            current = next_boundary

            if current > max_value:

                break

    ###########################################################################
    # NULL CHUNK
    #
    # ALWAYS LAST
    ###########################################################################

    chunks.append(

        Chunk(

            chunk_number=
                chunk_number,

            predicate=
                f"{driving_column} IS NULL",

            start_value=None,

            end_value=None,

            is_null_chunk=True
        )
    )

    return chunks


###############################################################################
# QUEUE BUILDER
###############################################################################

def build_chunk_queue(
    chunks
):

    q = queue.Queue()

    for chunk in chunks:

        q.put(chunk)

    return q


###############################################################################
# ESTIMATION
###############################################################################

def estimate_chunk_count(
    start_value,
    max_value,
    chunk_size,
    driving_column
):

    return len(

        generate_chunks(

            start_value,

            max_value,

            chunk_size,

            driving_column
        )
    )