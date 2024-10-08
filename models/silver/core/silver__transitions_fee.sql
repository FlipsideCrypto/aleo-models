{{ config(
    materialized = 'table',
    unique_key = "transition_id",
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        tx_id,
        block_timestamp,
        TRUE AS succeeded,
        fee_msg :transition AS transition
    FROM
        {{ ref('silver__transactions') }}
),
parsed AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        transition :id :: STRING AS transition_id,
        transition :program :: STRING AS program_id,
        transition :function :: STRING AS FUNCTION,
        TRY_PARSE_JSON(
            transition :inputs
        ) AS inputs,
        TRY_PARSE_JSON(
            transition :outputs
        ) AS outputs
    FROM
        base
),
fee_sum AS (
    SELECT
        transition_id,
        SUM(REPLACE(VALUE :value, 'u64') :: bigint) AS fee_raw,
        fee_raw / pow(
            10,
            6
        ) AS fee
    FROM
        parsed,
        LATERAL FLATTEN(inputs)
    WHERE
        VALUE :type = 'public'
        AND VALUE :value LIKE '%u64'
    GROUP BY
        transition_id
)
SELECT
    *
FROM
    parsed
    LEFT JOIN fee_sum USING (transition_id)
