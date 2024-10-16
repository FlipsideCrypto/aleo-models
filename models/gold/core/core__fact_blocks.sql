{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['fact_blocks_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core', 'full_test']
) }}

SELECT
    block_id,
    block_hash,
    block_timestamp,
    CASE
        WHEN network_id = 0 THEN 'mainnet'
    END AS network,
    tx_count,
    previous_hash,
    ROUND,
    rounds,
    coinbase_target,
    cumulative_proof_target,
    cumulative_weight,
    CASE WHEN block_id = 0 THEN 0 ELSE block_reward END AS block_reward,
    CASE WHEN block_id = 0 THEN 0 ELSE puzzle_reward END AS puzzle_reward,
    {{ dbt_utils.generate_surrogate_key(['block_id']) }} AS fact_blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__blocks') }}

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
