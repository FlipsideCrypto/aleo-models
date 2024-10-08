{{ config(
    materialized = 'table',
    unique_key = "transition_id",
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        *
    FROM
        {{ ref('silver__transactions') }}
),
exe_trans AS (
    SELECT
        t.block_id,
        t.tx_id,
        t.block_timestamp,
        TRUE AS succeeded,
        'execution' AS TYPE,
        f.value AS transition,
        f.index AS INDEX
    FROM
        base t,
        LATERAL FLATTEN(
            input => execution_msg :transitions
        ) f
),
rej_trans AS (
    SELECT
        t.block_id,
        t.tx_id,
        t.block_timestamp,
        FALSE AS succeeded,
        f.value: TYPE :: STRING AS TYPE,
        f.value AS transition,
        f.index AS transition_index
    FROM
        base t,
        LATERAL FLATTEN(
            input => rejected_msg :transitions
        ) f
),
transitions AS (
    SELECT
        *
    FROM
        exe_trans
    UNION ALL
    SELECT
        *
    FROM
        rej_trans
) {# ,
parsed AS (
    #}
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        INDEX,
        transition :id :: STRING AS transition_id,
        succeeded,
        TYPE,
        transition :program :: STRING AS program_id,
        transition :function :: STRING AS FUNCTION,
        TRY_PARSE_JSON(
            transition :inputs
        ) AS inputs,
        TRY_PARSE_JSON(
            transition :outputs
        ) AS outputs,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_id','transition_id']
        ) }} AS transitions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS invocation_id
    FROM
        transitions
