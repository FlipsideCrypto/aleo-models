{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core', 'full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        tx_id,
        INDEX,
        CASE
            WHEN status = 'accepted' THEN TRUE
            ELSE FALSE
        END AS tx_succeeded,
        tx_type,
        fee_msg,
        execution_msg,
        deployment_msg,
        owner_msg,
        finalize_msg,
        rejected_msg,
        transition_count
    FROM
        {{ ref('silver__transactions') }}

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
) {# ,
{{ dbt_utils.generate_surrogate_key(['block_id','network_id']) }} AS fact_blocks_id,
SYSDATE() AS inserted_timestamp,
SYSDATE() AS modified_timestamp,
'{{ invocation_id }}' AS _invocation_id #}
