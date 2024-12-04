{{ config(
    materialized = 'incremental',
    meta = { 'database_tags':{ 'table':{ 'PURPOSE': 'SWAPS' }}},
    unique_key = ['fact_swaps_id'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    cluster_by = ['block_timestamp::DATE', 'succeeded',' swapper', 'swap_from_name', 'swap_from_id', 'swap_to_name', 'swap_to_id', 'root_action'],
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['noncore', 'full_test']
) }}

WITH arcane AS (
    SELECT 
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        swapper,
        swap_from_amount,
        swap_from_name,
        swap_from_id,
        swap_to_amount,
        swap_to_name,
        swap_to_id,
        root_action,
        'Arcane Finance' as swap_protocol
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
    succeeded,
    swapper,
    swap_from_amount,
    swap_from_name,
    swap_from_id,
    swap_to_amount,
    swap_to_name,
    swap_to_id,
    root_action,
    swap_protocol,
    {{ dbt_utils.generate_surrogate_key(['tx_id','swap_protocol', 'swapper']) }} AS fact_swaps_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    arcane