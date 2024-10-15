{{ config(
    materialized = 'incremental',
    unique_key = ['dim_program_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core','full_test']
) }}

SELECT
    deployment_block_id,
    deployment_block_timestamp,
    program_id,
    edition,
    program,
    verifying_keys,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id']
    ) }} AS dim_program_id,
    SYSDATE() AS insert_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__programs') }}
UNION ALL
SELECT 
    * 
FROM 
    {{ ref('silver__custom_programs') }}