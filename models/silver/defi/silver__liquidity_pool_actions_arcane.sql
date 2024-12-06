{{ config(
    materialized = 'incremental',
    unique_key = 'liquidity_pool_actions_arcane_id',
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
        INPUTS
    FROM 
        {{ ref('core__fact_transitions') }}
    WHERE 
        program_id ILIKE 'arcn%'
        AND function NOT ILIKE '%credits%'
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
        program_id || '/' || function AS root_action,
        CASE 
            WHEN function ILIKE '%add%' THEN 'Add'
            WHEN function ILIKE '%remove%' THEN 'Remove'
            ELSE 'OTHER'
        END AS liquidity_action
    FROM
        base_transitions
    WHERE
        function ILIKE '%liq%'
),
reports AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INPUTS
    FROM
        base_transitions
    WHERE
        function = 'report'
),
parsed AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        root_action,
        liquidity_action,
        inputs[1]:value::string AS liquidity_provider,
        inputs[2]:value::string AS token1_id,
        inputs[3]:value::string AS token2_id,
        split_part(inputs[4]:value, 'u', 1) :: number AS token1_amount_raw,
        split_part(inputs[5]:value, 'u', 1) :: number AS token2_amount_raw
    FROM 
        root_actions
    JOIN
        reports USING(tx_id)
),
tokens AS (
    SELECT
        token_id,
        name_encoded,
        token_name,
        decimals
    FROM
        {{ ref('silver__token_registrations') }}
)
SELECT 
    p.block_timestamp,
    p.block_id,
    p.tx_id,
    p.succeeded,
    p.root_action,
    p.liquidity_action,
    p.liquidity_provider,
    p.token1_amount_raw as token1_amount_unadj,
    p.token1_amount_raw / power(10, t1.decimals) AS token1_amount,
    t1.symbol AS token1_symbol,
    p.token1_id,
    p.token2_amount_raw as token2_amount_unadj,
    p.token2_amount_raw / power(10, t2.decimals) AS token2_amount,
    t2.symbol AS token2_symbol,
    p.token2_id,
    {{ dbt_utils.generate_surrogate_key(['p.tx_id', 'p.token1_id', 'p.token2_id']) }} AS liquidity_pool_actions_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    parsed p
LEFT JOIN 
    {{ ref('silver__token_registrations') }} t1 ON p.token1_id = t1.token_id
LEFT JOIN 
    {{ ref('silver__token_registrations') }} t2 ON p.token2_id = t2.token_id
QUALIFY(ROW_NUMBER() OVER(PARTITION BY p.tx_id ORDER BY root_action) = 1)