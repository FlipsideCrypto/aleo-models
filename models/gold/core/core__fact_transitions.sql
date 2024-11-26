{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['fact_transitions_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE', 'program_id', 'function'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,transition_id,program_id,function,inputs,outputs);",
    tags = ['core', 'full_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    INDEX,
    transition_id,
    succeeded,
    TYPE,
    program_id,
    FUNCTION,
    inputs,
    outputs,
    {{ dbt_utils.generate_surrogate_key(
        ['transition_id']
    ) }} AS fact_transitions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transitions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= DATEADD(
        'minute',
        -5,
        (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    )
{% endif %}
