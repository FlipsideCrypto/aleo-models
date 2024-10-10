{{ config(
    materialized = 'incremental',
    unique_key = "program_id",
    tags = ['core','full_test'],
    enabled = false
) }}

WITH programs AS (

    SELECT
        program_id
    FROM
        {{ ref('silver__programs') }}
),
mappings AS (
    SELECT
        program_id,
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/program/' || program_id || '/mappings',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),{},
            'Vault/dev/aleo/mainnet'
        ) :data AS mappings
    FROM
        programs
)
SELECT
    program_id,
    mappings
FROM
    mappings
