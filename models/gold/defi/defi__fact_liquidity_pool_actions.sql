{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['fact_liquidity_pool_actions_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(liquidity_provider,token1_name,token2_name);",
    tags = ['noncore', 'full_test']
) }}

WITH arcane AS (
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        root_action,
        liquidity_action,
        liquidity_provider,
        token1_id,
        token2_id,
        token1_name,
        token2_name,
        token1_amount,
        token2_amount,
        'Arcane Finance' AS liquidity_pool_protocol
    FROM 
        {{ ref('silver__liquidity_pool_actions_arcane') }}

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
)

SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    root_action,
    liquidity_action,
    liquidity_provider,
    token1_id,
    token2_id,
    token1_name,
    token2_name,
    token1_amount,
    token2_amount,
    liquidity_pool_protocol,
    {{ dbt_utils.generate_surrogate_key(['tx_id', 'liquidity_pool_protocol', 'token1_id','token2_id']) }} AS fact_liquidity_pool_actions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    arcane -- union more later
