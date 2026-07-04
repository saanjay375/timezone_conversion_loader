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

Design Rules
------------
✓ No overlap between chunks
✓ No gaps between chunks
✓ Rows exactly equal to MAX(driving_column) must be included
✓ NULL driving-column rows processed last
✓ Supports D, W, M, Y chunk sizes
✓ Supports restart using startvalue

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
    """Chunk generation exception."""

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

    total_chunks: int = 0

    is_null_chunk: bool = False


###############################################################################
# STARTVALUE FORMATS
###############################################################################

SUPPORTED_STARTVALUE_FORMATS = ["%d-%m-%Y", "%d-%m-%Y %H:%M:%S"]


###############################################################################
# STARTVALUE CONVERSION
###############################################################################


def convert_startvalue(value: str) -> datetime:

    if value is None:

        raise ChunkGenerationError("startvalue cannot be NULL")

    value = value.strip()

    for fmt in SUPPORTED_STARTVALUE_FORMATS:

        try:

            return datetime.strptime(value, fmt)

        except ValueError:
            pass

    raise ChunkGenerationError(f"Invalid startvalue: {value}")


###############################################################################
# CHUNK SIZE PARSER
###############################################################################


def parse_chunk_size(chunk_size: str):

    if chunk_size is None:

        raise ChunkGenerationError("chunk_size cannot be NULL")

    chunk_size = chunk_size.strip()

    if len(chunk_size) < 2:

        raise ChunkGenerationError(f"Invalid chunk size: {chunk_size}")

    unit = chunk_size[-1].upper()

    try:

        value = int(chunk_size[:-1])

    except ValueError:

        raise ChunkGenerationError(f"Invalid chunk size: {chunk_size}")

    if value <= 0:

        raise ChunkGenerationError(f"Chunk size must be > 0: {chunk_size}")

    if unit not in ("D", "W", "M", "Y"):

        raise ChunkGenerationError(f"Unsupported chunk size: {chunk_size}")

    return value, unit


###############################################################################
# DATE HELPERS
###############################################################################


def add_months(source_date: datetime, months: int) -> datetime:

    month = source_date.month - 1 + months

    year = source_date.year + month // 12

    month = (month % 12) + 1

    day = min(source_date.day, monthrange(year, month)[1])

    return source_date.replace(year=year, month=month, day=day)


def next_day_start(dt: datetime) -> datetime:

    return datetime(dt.year, dt.month, dt.day) + timedelta(days=1)


def next_iso_week_start(dt: datetime) -> datetime:

    days_until_monday = 7 - dt.weekday()

    return datetime(dt.year, dt.month, dt.day) + timedelta(days=days_until_monday)


def next_month_start(dt: datetime) -> datetime:

    if dt.month == 12:

        return datetime(dt.year + 1, 1, 1)

    return datetime(dt.year, dt.month + 1, 1)


def next_year_start(dt: datetime) -> datetime:

    return datetime(dt.year + 1, 1, 1)


def first_aligned_boundary(current_date: datetime, chunk_unit: str) -> datetime:

    if chunk_unit == "D":
        return next_day_start(current_date)

    if chunk_unit == "W":
        return next_iso_week_start(current_date)

    if chunk_unit == "M":
        return next_month_start(current_date)

    if chunk_unit == "Y":
        return next_year_start(current_date)

    raise ChunkGenerationError(f"Unsupported chunk unit: {chunk_unit}")


def advance_aligned_boundary(
    boundary: datetime, chunk_value: int, chunk_unit: str
) -> datetime:

    if chunk_unit == "D":
        return boundary + timedelta(days=chunk_value)

    if chunk_unit == "W":
        return boundary + timedelta(weeks=chunk_value)

    if chunk_unit == "M":
        return add_months(boundary, chunk_value)

    if chunk_unit == "Y":
        return add_years(boundary, chunk_value)

    raise ChunkGenerationError(f"Unsupported chunk unit: {chunk_unit}")


def add_years(source_date: datetime, years: int) -> datetime:

    try:

        return source_date.replace(year=source_date.year + years)

    except ValueError:

        #
        # Leap-year adjustment.
        #

        return source_date.replace(month=2, day=28, year=source_date.year + years)


###############################################################################
# NEXT BOUNDARY
###############################################################################


def compute_next_boundary(
    current_date: datetime, chunk_value: int, chunk_unit: str
) -> datetime:

    if chunk_unit == "D":

        return current_date + timedelta(days=chunk_value)

    if chunk_unit == "W":

        return current_date + timedelta(weeks=chunk_value)

    if chunk_unit == "M":

        return add_months(current_date, chunk_value)

    if chunk_unit == "Y":

        return add_years(current_date, chunk_value)

    raise ChunkGenerationError(f"Unsupported chunk unit: {chunk_unit}")


###############################################################################
# PREDICATE GENERATOR
###############################################################################


def build_chunk_predicate(start_value: datetime, end_value: datetime) -> str:

    return f">={start_value} " f"<{end_value}"


###############################################################################
# CHUNK GENERATION
###############################################################################


def generate_chunks(
    start_value: datetime, max_value: datetime, chunk_size: str, driving_column: str
):

    if start_value is None:

        raise ChunkGenerationError("start_value cannot be NULL")

    if max_value is None:

        raise ChunkGenerationError("max_value cannot be NULL")

    chunk_value, chunk_unit = parse_chunk_size(chunk_size)

    chunks = []

    chunk_number = 1

    max_boundary = max_value + timedelta(microseconds=1)

    ###########################################################################
    # SPECIAL CASE
    #
    # MIN == MAX
    ###########################################################################

    if start_value == max_value:

        chunks.append(
            Chunk(
                chunk_number=chunk_number,
                predicate=build_chunk_predicate(start_value, max_boundary),
                start_value=start_value,
                end_value=max_boundary,
                is_null_chunk=False,
            )
        )

        chunk_number += 1

    ###########################################################################
    # NORMAL PATH
    ###########################################################################

    else:

        first_boundary = first_aligned_boundary(start_value, chunk_unit)

        #######################################################################
        # CASE 1
        #
        # Entire range fits before first aligned boundary.
        #
        # Example:
        # start = 14-Feb-2024
        # max   = 15-Feb-2024
        # chunk = 1Y
        #######################################################################

        if first_boundary >= max_boundary:

            chunks.append(
                Chunk(
                    chunk_number=chunk_number,
                    predicate=build_chunk_predicate(start_value, max_boundary),
                    start_value=start_value,
                    end_value=max_boundary,
                    is_null_chunk=False,
                )
            )

            chunk_number += 1

        else:

            ###################################################################
            # FIRST CHUNK
            #
            # Actual start -> first aligned boundary
            ###################################################################

            chunks.append(
                Chunk(
                    chunk_number=chunk_number,
                    predicate=build_chunk_predicate(start_value, first_boundary),
                    start_value=start_value,
                    end_value=first_boundary,
                    is_null_chunk=False,
                )
            )

            chunk_number += 1

            current_boundary = first_boundary

            ###################################################################
            # ALIGNED CHUNKS
            ###################################################################

            while current_boundary < max_boundary:

                next_boundary = advance_aligned_boundary(
                    current_boundary, chunk_value, chunk_unit
                )

                actual_end = min(next_boundary, max_boundary)

                chunks.append(
                    Chunk(
                        chunk_number=chunk_number,
                        predicate=build_chunk_predicate(current_boundary, actual_end),
                        start_value=current_boundary,
                        end_value=actual_end,
                        is_null_chunk=False,
                    )
                )

                chunk_number += 1

                if actual_end >= max_boundary:

                    break

                current_boundary = next_boundary

    ###########################################################################
    # NULL CHUNK
    #
    # ALWAYS LAST
    ###########################################################################

    chunks.append(
        Chunk(
            chunk_number=chunk_number,
            predicate=f"{driving_column} IS NULL",
            start_value=None,
            end_value=None,
            is_null_chunk=True,
        )
    )

    total_chunks = len(chunks)

    for chunk in chunks:

        chunk.total_chunks = total_chunks

    return chunks


###############################################################################
# QUEUE BUILDER
###############################################################################


def build_chunk_queue(chunks):

    chunk_queue = queue.Queue()

    for chunk in chunks:

        chunk_queue.put(chunk)

    return chunk_queue


###############################################################################
# CHUNK ESTIMATION
###############################################################################


def estimate_chunk_count(start_value, max_value, chunk_size, driving_column):

    chunks = generate_chunks(start_value, max_value, chunk_size, driving_column)

    return len(chunks)
