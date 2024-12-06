{{ config(
    materialized = 'incremental',
    unique_key = 'swaps_arcane_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore', 'full_test']
) }}

-- depends on {{ ref('core__fact_transitions') }}
WITH base_transitions AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        function,
        inputs,
        index,
        modified_timestamp
    FROM
        {{ ref('core__fact_transitions') }}
    WHERE
        program_id ILIKE 'arcn%'
    {% if is_incremental() %}
    AND modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM
            {{ this }}
    )
    {% endif %}
),
root_actions AS (
    SELECT
        tx_id,
        program_id || '/' || function AS root_action
    FROM
        base_transitions
    WHERE
        function ILIKE '%swap%'
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY index DESC) = 1
),
reports AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        REPLACE(inputs[0]:value, 'field', '') AS address_from,
        REPLACE(inputs[1]:value, 'field', '') AS address_to,
        inputs[2]:value AS swap_from,
        inputs[3]:value AS swap_to,
        REPLACE(inputs[4]:value, 'u128', '') AS amount_from,
        REPLACE(inputs[5]:value, 'u128', '') AS amount_to
    FROM
        base_transitions
    WHERE
        program_id = 'arcn_compliance_v1.aleo'
        AND function = 'report'
),
agg AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        root_action,
        address_from,
        address_to,
        swap_from,
        swap_to,
        amount_from :: float AS amount_from,
        amount_to :: float AS amount_to
    FROM
        reports
    JOIN
        root_actions USING(tx_id)
)

SELECT 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.succeeded,
    a.address_from AS swapper,
    a.amount_from as from_amount_unadj,
    a.amount_from / power(10, t1.decimals) AS from_amount,
    t1.symbol AS from_symbol,
    a.swap_from AS from_id,
    a.amount_to as to_amount_unadj,
    a.amount_to / power(10, t2.decimals) AS to_amount,
    t2.symbol AS to_symbol,
    a.swap_to AS to_id,
    a.root_action,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 't1.token_id']) }} AS swaps_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    agg a
LEFT JOIN 
    {{ ref('silver__token_registrations') }} t1 ON a.swap_from = t1.token_id
LEFT JOIN 
    {{ ref('silver__token_registrations') }} t2 ON a.swap_to = t2.token_id