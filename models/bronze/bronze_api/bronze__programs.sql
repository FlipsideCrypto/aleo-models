{{ config(
    materialized = 'table',
    unique_key = "program_id",
    tags = ['core','full_test']
) }}

WITH programs AS (
    SELECT
        block_id,
        block_timestamp,
        REGEXP_SUBSTR(tx_data :program :: STRING, 'program\\s+(\\S+);', 1, 1, 'e', 1) AS program_id,
        tx_data
    FROM 
        {{ ref('silver__transactions') }}
    WHERE
        tx_type = 'deploy'
),
mappings AS (
    SELECT
        block_id,
        block_timestamp,
        program_id,
        {{ target.database }}.live.udf_api(
            'GET',
            '{Service}/program/' || program_id || '/mappings',
            OBJECT_CONSTRUCT(
                'Content-Type',
                'application/json'
            ),{},
            'Vault/dev/aleo/mainnet'
        ) :data AS mappings,
        tx_data
    FROM programs
)
SELECT
    block_id,
    block_timestamp,
    program_id,
    mappings,
    tx_data
FROM mappings