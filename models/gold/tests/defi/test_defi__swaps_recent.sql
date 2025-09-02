{{ config (
    materialized = 'view',
    tags = ['recent_test']
) }}

WITH last_7_days AS (

    SELECT
        block_date
    FROM
        {{ ref("_max_block_by_date") }}
        qualify ROW_NUMBER() over (
            ORDER BY
                block_date DESC
        ) = 7
)
SELECT
    *
FROM
    {{ ref('defi__fact_swaps') }}
WHERE
    block_timestamp :: DATE >= (
        SELECT
            block_date
        FROM
            last_7_days
    )