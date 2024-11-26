{{ config(
    materialized = 'incremental',
    meta = { 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    cluster_by = ['block_timestamp::DATE', 'input_token_id', 'output_token_id', 'swapper', 'is_private'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['noncore', 'full_test']
) }}

WITH arcane AS (
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        pool_id,
        initiator_address,
        input_amount,
        input_token_id,
        output_amount,
        output_token_id,
        involved_programs,
        involved_functions,
        first_index,
        last_index,
        is_private
    FROM 
        {{ ref('silver__swaps_arcane') }}
    {% if is_incremental() %}
    WHERE
        modified_timestamp >= DATEADD(
            'minute',
            -5,(
                SELECT
                    MAX(
                        modified_timestamp
                    )
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
    pool_id,
    initiator_address as swapper,
    input_amount,
    input_token_id,
    output_amount,
    output_token_id,
    involved_programs,
    involved_functions,
    first_index,
    last_index,
    is_private,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','swapper']) }} AS fact_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    arcane