{{ config(
    materialized = 'incremental',
    unique_key = 'liquidity_pool_actions_arcane_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['noncore', 'full_test']
) }}

WITH root_actions AS (
    SELECT
        tx_id,
        program_id,
        function,
        program_id || '/' || function AS root_action,
        CASE 
            WHEN function ILIKE '%add%' THEN 'Add'
            WHEN function ILIKE '%remove%' THEN 'Remove'
            ELSE 'OTHER'
        END AS liquidity_action
    FROM
        {{ ref('core__fact_transitions') }}
    WHERE
        program_id ILIKE 'arcn%'
        AND function ILIKE '%liq%'
        AND function NOT ILIKE '%credits%'
),

reports AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INPUTS
    FROM
        {{ ref('core__fact_transitions') }}
    WHERE
        program_id ILIKE 'arcn%'
        AND function = 'report'
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
        split_part(inputs[4]:value, 'u', 1)::number AS token1_amount,
        split_part(inputs[5]:value, 'u', 1)::number AS token2_amount
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
    p.token1_amount / power(10, COALESCE(t1.decimals, 6)) AS token1_amount,
    CASE
        WHEN p.token1_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' THEN 'Aleo'
        ELSE t1.token_name
    END AS token1_name,
    p.token1_id,
    p.token2_amount / power(10, COALESCE(t2.decimals, 6)) AS token2_amount,
    CASE
        WHEN p.token2_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' THEN 'Aleo'
        ELSE t2.token_name
    END AS token2_name,
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