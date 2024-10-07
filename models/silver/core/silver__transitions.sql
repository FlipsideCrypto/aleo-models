{{ config(
    materialized = 'table',
    unique_key = "transition_id",
    tags = ['core','full_test']
) }}

WITH base AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        tx_data
    FROM
        {{ ref('silver__transactions') }}
),
transitions AS (
  SELECT 
    t.block_id,
    t.tx_id,
    t.block_timestamp,
    f.value AS transition,
    f.index AS transition_index
  FROM base t,
    TABLE(FLATTEN(input => PARSE_JSON(tx_data):transitions)) f
    WHERE block_id is not null
),
parsed AS (
    SELECT 
        block_id,
        block_timestamp,
        tx_id,
        transition :id :: STRING AS transition_id,
        transition :program :: STRING AS program_id,
        transition :function :: STRING AS transition_function,
        transition :inputs :: STRING AS transition_inputs,
        transition :outputs :: STRING AS transition_outputs
    FROM transitions
)
SELECT
    block_id,
    block_timestamp,
    tx_id,
    transition_id,
    program_id,
    transition_function,
    transition_inputs,
    transition_outputs,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id','transition_id']
    ) }} AS complete_transition_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM parsed