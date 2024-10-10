{{ config(
    materialized = 'incremental',
    unique_key = "tx_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        f.value :index :: INT AS INDEX,
        f.value :transaction :id :: STRING AS tx_id,
        f.value :status :: STRING AS status,
        f.value :type :: STRING AS tx_type,
        TRY_PARSE_JSON(
            f.value :transaction :fee
        ) AS fee_msg,
        TRY_PARSE_JSON(
            f.value :transaction :execution
        ) AS execution_msg,
        TRY_PARSE_JSON(
            f.value :transaction :deployment
        ) AS deployment_msg,
        TRY_PARSE_JSON(
            f.value :transaction :owner
        ) AS owner_msg,
        TRY_PARSE_JSON(
            f.value: finalize
        ) AS finalize_msg,
        TRY_PARSE_JSON(
            f.value: rejected
        ) AS rejected_msg,
        f.value AS DATA
    FROM
        {{ ref('silver__blocks') }}
        t,
        LATERAL FLATTEN(
            input => t.data :transactions
        ) f
    WHERE
        tx_id IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp >= DATEADD(
    MINUTE,
    -5,(
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
)
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    INDEX,
    tx_id,
    status,
    tx_type,
    fee_msg,
    execution_msg,
    deployment_msg,
    owner_msg,
    finalize_msg,
    rejected_msg,
    COALESCE(ARRAY_SIZE(execution_msg :transitions), ARRAY_SIZE(rejected_msg :execution :transitions)) AS transition_count,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
