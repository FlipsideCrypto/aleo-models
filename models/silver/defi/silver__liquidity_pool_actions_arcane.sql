{{ config(
    materialized='incremental',
    unique_key='liquidity_pool_actions_arcane_id',
    incremental_strategy='merge',
    cluster_by=['modified_timestamp::DATE'],
    tags=['noncore', 'full_test']
) }}
-- depends_on: {{ ref('core__fact_transitions') }}
WITH base AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        index,
        program_id,
        function,
        PARSE_JSON(inputs) AS json_inputs
    FROM 
        {{ ref('core__fact_transitions') }}
    WHERE
        succeeded
        AND program_id IN (
            'arcn_pool_v2_2_2.aleo',
            'arcn_puc_in_helper_v2_2_3.aleo',
            'arcn_puc_in_helper_v2_2_4.aleo',
            'arcn_puc_out_helper_v2_2_3.aleo',
            'arcn_puc_out_helper_v2_2_4.aleo'
        )
        AND function IN (
            'add_amm_liquidity',
            'remove_amm_liquidity_part',
            'add_amm_liq_credits_is_token1',
            'add_amm_liq_credits_is_token2',
            'remove_liq_credits_is_token1',
            'remove_liq_credits_is_token2'
        )
    {% if is_incremental() %}
    AND
        modified_timestamp >= (
            SELECT
                MAX(
                    modified_timestamp
                )
            FROM
                {{ this }}
        )
    {% endif %}
),
extracted_data AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        index,
        program_id,
        function,
        json_inputs[1]:value::string AS initiator_address,
        CASE 
            WHEN function IN ('add_amm_liquidity', 'add_amm_liq_credits_is_token1', 'add_amm_liq_credits_is_token2') THEN json_inputs[6]:value::string
            WHEN function IN ('remove_amm_liquidity_part', 'remove_liq_credits_is_token1', 'remove_liq_credits_is_token2') THEN json_inputs[2]:value::string
        END AS liquidity_pool_address,
        json_inputs[7]:value::string AS token1_id,
        json_inputs[8]:value::string AS token2_id,
        split_part(json_inputs[3]:value::string, 'u', 1) AS token1_amount,
        split_part(json_inputs[5]:value::string, 'u', 1) AS token2_amount,
        json_inputs[1]:value::string AS liquidity_provider,
        CASE WHEN 
            initiator_address ilike 'ciphertext%' THEN TRUE
            WHEN token1_amount ilike 'ciphertext%' THEN TRUE
            WHEN token2_amount = 'false' THEN TRUE
            WHEN liquidity_provider ilike 'ciphertext%' THEN TRUE
            WHEN liquidity_pool_address ilike 'ciphertext%' THEN TRUE
            ELSE FALSE
        END AS is_private
    FROM 
        base
    WHERE 
        json_inputs IS NOT NULL
)

SELECT 
    block_timestamp,
    block_id,
    tx_id,
    index,
    program_id,
    function,
    CASE WHEN 
        token1_id ILIKE 'ciphertext%' OR token1_id = 'false' THEN NULL 
        ELSE token1_id 
    END AS token1_id,
    CASE WHEN 
        token2_id ILIKE 'ciphertext%' OR token2_id = 'false' THEN NULL 
        ELSE token2_id 
    END AS token2_id,
    CASE WHEN 
        initiator_address ILIKE 'ciphertext%' OR initiator_address = 'false' THEN NULL 
        ELSE initiator_address 
    END AS initiator_address,
    CASE WHEN 
        token1_amount ILIKE 'ciphertext%' OR token1_amount = 'false' THEN NULL 
        ELSE token1_amount 
    END AS token1_amount,
    CASE WHEN 
        token2_amount ILIKE 'ciphertext%' OR token2_amount = 'false' THEN NULL 
        ELSE token2_amount 
    END AS token2_amount,
    CASE WHEN 
        liquidity_provider ILIKE 'ciphertext%' OR liquidity_provider = 'false' THEN NULL 
        ELSE liquidity_provider 
    END AS liquidity_provider,
    CASE WHEN 
        liquidity_pool_address ILIKE 'ciphertext%' OR liquidity_pool_address = 'false' THEN NULL 
        ELSE liquidity_pool_address 
    END AS liquidity_pool_address,
    is_private,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','INDEX']) }} AS liquidity_pool_actions_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM 
    extracted_data