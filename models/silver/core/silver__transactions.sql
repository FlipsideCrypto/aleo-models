{{ config(
    materialized = 'table',
    unique_key = "tx_id",
    tags = ['core','full_test']
) }}

WITH base AS (
    SELECT
        t.VALUE :BLOCK_ID_REQUESTED :: INT AS block_id,
        t.DATA :id :: STRING as tx_id,
        t.DATA :type AS tx_type,
        CASE WHEN f.key :: STRING NOT IN ('id', 'type', 'fee') THEN f.value END AS tx_msg,
        CASE WHEN f.key :: STRING = 'fee' THEN f.value END AS fee_msg,
        t.DATA,
        t.PARTITION_KEY
    FROM
        {% if is_incremental() %}
        {{ ref('bronze__transactions') }} t,
        {% else %}
        {{ ref('bronze__transactions_FR') }} t,
        {% endif %}
        LATERAL FLATTEN(input => t.DATA) AS f

    {% if is_incremental() %}
    WHERE inserted_timestamp >= DATEADD(
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
detail AS (
    SELECT
        block_id,
        tx_id,
        MAX(COALESCE(tx_type, '')) AS tx_type,
        PARSE_JSON(MAX(COALESCE(tx_msg, ''))) AS tx_msg,
        PARSE_JSON(MAX(COALESCE(fee_msg, ''))) AS fee_msg,
        DATA,
        PARTITION_KEY
    FROM base
    GROUP BY ALL
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    tx_type,
    CASE 
        WHEN tx_type = 'fee' then fee_msg
        ELSE tx_msg
    END as tx_data,
    fee_msg,
    COALESCE(ARRAY_SIZE(tx_data :transitions) :: NUMBER, 0) AS tx_transition_count,
    CASE 
        WHEN tx_type = 'fee' then fee_msg :transition
        ELSE tx_msg :transitions
    END as tx_transitions,
    fee_msg :proof :: STRING AS fee_proof,
    fee_msg :global_state_root :: STRING AS fee_global_state_root,
    fee_msg :transition :id :: STRING AS fee_transition_id,
    fee_msg :transition :program :: STRING AS fee_transition_program,
    fee_msg :transition :function :: STRING AS fee_transition_function,
    fee_msg :transition :inputs :: STRING AS fee_transition_inputs,
    fee_msg :transition :outputs :: STRING AS fee_transition_outputs,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id']
    ) }} AS complete_transactions_id,
    partition_key,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    detail
JOIN (
    SELECT
        block_id,
        block_timestamp
    FROM
        {{ ref('silver__blocks') }}
    WHERE
        tx_count > 0
    ) b
USING(block_id)

