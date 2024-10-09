{{ config(
    materialized = 'incremental',
    unique_key = ['program_id'],
    incremental_strategy = 'merge',
    tags = ['core','full_test']
) }}

SELECT
    deployment_block_id,
    deployment_block_timestamp,
    program_id,
    edition,
    program,
    {# mappings, #}
    verifying_keys,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id']
    ) }} AS dim_program_id,
    SYSDATE() AS insert_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__programs') }}
