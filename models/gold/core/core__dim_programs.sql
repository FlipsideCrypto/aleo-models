{{ config(
    materialized = 'incremental',
    unique_key = ['dim_program_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core','full_test']
) }}

WITH base AS (
    SELECT
        deployment_block_id,
        deployment_block_timestamp,
        program_id,
        edition,
        program,
        verifying_keys,
        {{ dbt_utils.generate_surrogate_key(
            ['program_id', 'edition']
        ) }} AS dim_program_id,
        SYSDATE() AS insert_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id
    FROM
        {{ ref('silver__programs') }}

    {% if is_incremental() %}
    WHERE
        modified_timestamp >= (
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
        )
        {% endif %}
),

custom_programs AS (
    SELECT 
        * 
    FROM 
        {{ ref('silver__custom_programs') }}
)
SELECT * FROM base
UNION ALL
SELECT * FROM custom_programs