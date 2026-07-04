from datetime import datetime

from chunking import (
    next_day_start,
    next_iso_week_start,
    next_month_start,
    next_year_start,
    first_aligned_boundary,
    advance_aligned_boundary,
    add_years,
    generate_chunks,
)

print("=" * 80)
print("STEP-1 TESTS")
print("=" * 80)

print("DAY:", next_day_start(datetime(2024, 2, 14, 6, 14, 56)))

print("WEEK:", next_iso_week_start(datetime(2024, 2, 14, 6, 14, 56)))

print("MONTH:", next_month_start(datetime(2024, 2, 1, 00, 00, 00)))

print("YEAR:", next_year_start(datetime(2024, 2, 14, 6, 14, 56)))

print()

print("=" * 80)
print("STEP-2 TESTS")
print("=" * 80)

print(
    "FIRST MONTH:",
    first_aligned_boundary(
        datetime(2024, 2, 1, 00, 00, 00),
        "M",
    ),
)

print(
    "FIRST WEEK:",
    first_aligned_boundary(
        datetime(2024, 2, 14, 6, 14, 56),
        "W",
    ),
)

print(
    "FIRST YEAR:",
    first_aligned_boundary(
        datetime(2024, 2, 14, 6, 14, 56),
        "Y",
    ),
)

print()

print(
    "ADVANCE 3 MONTHS:",
    advance_aligned_boundary(
        datetime(2024, 2, 1, 00, 00, 00),
        3,
        "M",
    ),
)

print(
    "ADVANCE 2 WEEKS:",
    advance_aligned_boundary(
        datetime(2024, 2, 19, 0, 0, 0),
        2,
        "W",
    ),
)

print(
    "ADVANCE 2 YEARS:",
    advance_aligned_boundary(
        datetime(2024, 1, 1, 0, 0, 0),
        2,
        "Y",
    ),
)

print()

print("=" * 80)
print("LEAP YEAR TESTS")
print("=" * 80)

print("LEAP DAY -> NEXT MONTH:", next_month_start(datetime(2024, 2, 29, 10, 15, 0)))

print("LEAP DAY -> NEXT YEAR START:", next_year_start(datetime(2024, 2, 29, 10, 15, 0)))

print(
    "LEAP DAY + 1 YEAR:",
    add_years(
        datetime(2024, 2, 29, 10, 15, 0),
        1,
    ),
)

print()

print("=" * 80)
print("MONTH-END TESTS")
print("=" * 80)

print("31-JAN -> NEXT MONTH:", next_month_start(datetime(2024, 1, 31, 23, 59, 59)))

print("31-DEC -> NEXT YEAR:", next_year_start(datetime(2024, 12, 31, 23, 59, 59)))

chunks = generate_chunks(
    datetime(2021, 12, 13, 6, 14, 56),
    datetime(2022, 10, 15, 12, 0, 0),
    "3M",
    "pxcommitdatetime",
)

for c in chunks:

    print(c.chunk_number, c.start_value, c.end_value, c.is_null_chunk)
