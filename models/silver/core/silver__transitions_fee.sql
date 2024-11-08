{{ config(
    materialized = 'incremental',
    unique_key = "transitions_fee_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        tx_id,
        block_timestamp,
        TRUE AS succeeded,
        fee_msg :transition AS transition
    FROM
        {{ ref('silver__transactions') }}
    WHERE 
        fee_msg IS NOT NULL

{% if is_incremental() %}
AND
    modified_timestamp >= DATEADD(
        MINUTE,
        -5,(
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    )
{% endif %}
),
parsed AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        succeeded,
        transition :id :: STRING AS transition_id,
        transition :program :: STRING AS program_id,
        transition :function :: STRING AS FUNCTION,
        TRY_PARSE_JSON(
            transition :inputs
        ) AS inputs,
        TRY_PARSE_JSON(
            transition :outputs
        ) AS outputs
    FROM
        base
),
fee_sum AS (
    SELECT
        transition_id,
        SUM(REPLACE(VALUE :value, 'u64') :: bigint) AS fee_raw,
        fee_raw / pow(
            10,
            6
        ) AS fee
    FROM
        parsed,
        LATERAL FLATTEN(inputs)
    WHERE
        VALUE :type = 'public'
        AND VALUE :value LIKE '%u64'
    GROUP BY
        transition_id
),
fee_payer AS (
    SELECT
        transition_id,
        REGEXP_SUBSTR( 
            outputs[array_size(outputs)-1] :value :: STRING, 
            'arguments:\\s*\\[(.*?)\\]', 1, 1, 'sie' ) as args_string, 
        SPLIT(
            REGEXP_REPLACE( REGEXP_REPLACE( REGEXP_REPLACE(args_string, '\\s+', ''), '\\[|\\]', '' ), 'u64$', '' ), ',' ) aa, 
        aa[0]::STRING as fee_payer
    FROM
        parsed
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    succeeded,
    transition_id,
    program_id,
    FUNCTION,
    inputs,
    outputs,
    fee_raw,
    fee,
    fee_payer,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','transition_id']
    ) }} AS transitions_fee_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed
    LEFT JOIN fee_sum USING (transition_id)
    LEFT JOIN fee_payer USING (transition_id)