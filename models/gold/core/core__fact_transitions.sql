{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "transition_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core', 'full_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    transition_id,
    program_id,
    transition_function,
    transition_inputs,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','transition_id']
    ) }} AS complete_transition_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transitions') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= DATEADD(
        'hour',
        -1,
        (SELECT MAX(block_timestamp) FROM {{ this }})
    )
{% endif %}