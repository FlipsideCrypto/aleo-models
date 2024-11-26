{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['fact_liquidity_pool_actions_arcane_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(liquidity_pool_address,initiator_address,token1_id,token2_id);",
    tags = ['noncore', 'full_test']
) }}

WITH arcane AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        program_id,
        function,
        token1_id,
        token2_id,
        initiator_address,
        CASE WHEN token1_amount ILIKE '%field%' THEN NULL ELSE token1_amount :: number END AS token1_amount,
        CASE WHEN token2_amount ILIKE '%field%' THEN NULL ELSE token2_amount :: number END AS token2_amount,
        liquidity_provider,
        liquidity_pool_address,
        is_private
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
    index,
    program_id,
    function,
    token1_id,
    token2_id,
    initiator_address,
    token1_amount,
    token2_amount,
    liquidity_provider,
    liquidity_pool_address,
    is_private,
    {{ dbt_utils.generate_surrogate_key(['block_id','tx_id','index']) }} AS fact_liquidity_pool_actions_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    arcane -- union more later
