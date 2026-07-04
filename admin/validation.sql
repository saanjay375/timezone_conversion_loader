
SELECT
    s.pxinsname,

    s.pxcommitdatetime AS source_time,

    s.pxcommitdatetime
        AT TIME ZONE 'America/New_York'
        AT TIME ZONE 'UTC'
        AS expected_utc,

    t.pxcommitdatetime AS actual_utc

FROM repack.pr_index_test s
JOIN repack.pr_index_test_utc t
    ON s.pxinsname = t.pxinsname
LIMIT 20;


SELECT
    COUNT(*) AS mismatched_rows
FROM repack.pr_index_test s
JOIN repack.pr_index_test_utc t
    ON s.pxinsname = t.pxinsname
WHERE

COALESCE(
    s.pxcommitdatetime
        AT TIME ZONE 'America/New_York'
        AT TIME ZONE 'UTC',
    TIMESTAMP '1900-01-01'
)
<>
COALESCE(
    t.pxcommitdatetime,
    TIMESTAMP '1900-01-01'
)

OR

COALESCE(
    s.pxcreatedatetime
        AT TIME ZONE 'America/New_York'
        AT TIME ZONE 'UTC',
    TIMESTAMP '1900-01-01'
)
<>
COALESCE(
    t.pxcreatedatetime,
    TIMESTAMP '1900-01-01'
)

OR

COALESCE(
    s.pxsavedatetime
        AT TIME ZONE 'America/New_York'
        AT TIME ZONE 'UTC',
    TIMESTAMP '1900-01-01'
)
<>
COALESCE(
    t.pxsavedatetime,
    TIMESTAMP '1900-01-01'
)

OR

COALESCE(
    s.pxupdatedatetime
        AT TIME ZONE 'America/New_York'
        AT TIME ZONE 'UTC',
    TIMESTAMP '1900-01-01'
)
<>
COALESCE(
    t.pxupdatedatetime,
    TIMESTAMP '1900-01-01'
);
