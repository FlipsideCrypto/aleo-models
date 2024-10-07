{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['program_id'],
    incremental_strategy = 'merge',
    cluster_by = 'deployment_block_timestamp::DATE',
    tags = ['core','full_test']
) }}

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
    ) }} AS complete_program_id,
    SYSDATE() AS insert_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM {{ ref('silver__programs') }}