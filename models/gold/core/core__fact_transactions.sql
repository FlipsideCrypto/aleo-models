{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "transaction_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core', 'full_test']
) }}

SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_type,
    tx_data,
    tx_transition_count,
    fee_proof,
    fee_global_state_root,
    fee_transition_id,
    fee_transition_program,
    fee_transition_function,
    fee_transition_inputs,
    fee_transition_outputs,
    complete_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }}

{% if is_incremental() %}
WHERE
    block_timestamp >= DATEADD(
        'hour',
        -1,
        (SELECT MAX(block_timestamp) FROM {{ this }})
    )
{% endif %}