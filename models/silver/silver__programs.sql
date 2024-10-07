{{ config(
    materialized = 'table',
    unique_key = "program_id",
    tags = ['core','full_test']
) }}

WITH base as (
    SELECT
        block_id,
        block_timestamp,
        tx_data,
        mappings
    FROM 
        {{ ref('bronze__programs') }}
),
parsed AS (
    SELECT
        block_id AS deployment_block_id,
        block_timestamp AS deployment_block_timestamp,
        REGEXP_SUBSTR(tx_data :program :: STRING, 'program\\s+(\\S+);', 1, 1, 'e', 1) AS program_id,
        tx_data :edition :: INT AS edition,
        tx_data :program :: STRING AS program,
        tx_data :verifying_keys :: STRING AS verifying_keys,
        mappings
    FROM base
)
SELECT
    deployment_block_id,
    deployment_block_timestamp,
    program_id,
    edition,
    program,
    mappings,
    verifying_keys,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id']
    ) }} AS complete_program_id
FROM 
    parsed
ORDER BY
    program_id