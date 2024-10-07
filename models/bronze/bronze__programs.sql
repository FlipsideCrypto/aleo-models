{{ config(
    materialized = 'table',
    unique_key = "program_id",
    tags = ['core','full_test']
) }}

WITH program_ids AS (
    SELECT DISTINCT
        program_id
    FROM {{ ref('silver__transitions') }}
),
programs AS (
SELECT
    program_id,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/program/' || program_id,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/dev/aleo/mainnet'
    ) AS program
FROM
    program_ids
),
mappings AS (
SELECT
    program_id,
    program,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/program/' || program_id || '/mappings',
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/dev/aleo/mainnet'
    ) AS mappings
FROM programs
)
SELECT
    program_id,
    program,
    mappings,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id']
    ) }} AS complete_program_id
FROM mappings