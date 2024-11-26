{{ config(
    materialized='incremental',
    unique_key='swaps_arcane_id',
    incremental_strategy='merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE', 'token_id'],
    tags=['noncore', 'full_test']
) }}

-- depends on: {{ ref('core__fact_transitions') }}
WITH parsed_data AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        function,
        index,
        PARSE_JSON(INPUTS) AS json_inputs
    FROM 
        {{ ref('core__fact_transitions') }}
    WHERE
        succeeded
        AND program_id IN (
            'arcn_priv_v2_2_2.aleo',
            'arcn_pool_v2_2_2.aleo',
            'arcn_pub_v2_2_3.aleo',
            'arcn_credits_out_helper_v2_2_3.aleo',
            'arcn_credits_in_helper_v2_2_3.aleo',
            'arcn_puc_in_helper_v2_2_4.aleo',
            'arcn_puc_out_helper_v2_2_3.aleo'
        )
        AND function IN (
            'swap_amm',
            'swap_amm_credits_in',
            'swap_amm_credits_out'
        )
    {% if is_incremental() %}
    WHERE
        modified_timestamp >= DATEADD(
            'minute',
            -5,(
                SELECT
                    MAX(
                        modified_timestamp
                    )
                FROM
                    {{ this }}
            )
        )
    {% endif %}
),
mapped_swaps AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        program_id,
        function,
        index,
        CASE 
            WHEN program_id IN (
                'arcn_priv_v2_2_2.aleo', 
                'arcn_pool_v2_2_2.aleo', 
                'arcn_pub_v2_2_3.aleo'
            ) AND function = 'swap_amm'
                THEN json_inputs[0]:value::string
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_3.aleo', 
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out'
                THEN json_inputs[0]:value::string
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_3.aleo', 
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in'
                THEN json_inputs[0]:value::string
        END AS token_id,
        CASE
            WHEN program_id IN (
                'arcn_priv_v2_2_2.aleo', 
                'arcn_pool_v2_2_2.aleo', 
                'arcn_pub_v2_2_3.aleo'
            ) AND function = 'swap_amm'
                THEN json_inputs[1]:value::string
            ELSE json_inputs[1]:value::string
        END AS initiator_address,
        json_inputs[3]:value::string AS input_amount,
        json_inputs[5]:value::string AS output_amount,
        json_inputs[6]:value::string AS pool_id
    FROM parsed_data
),
cleaned AS (
    SELECT
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    function,
    index,
    token_id,
    CASE WHEN 
        initiator_address ilike 'aleo%' then initiator_address
        ELSE NULL
    END AS initiator_address,
    CASE WHEN 
        input_amount ilike 'ciphertext%' THEN NULL
        WHEN input_amount = 'false' THEN NULL
        WHEN input_amount ilike '%field' THEN NULL
        ELSE split_part(input_amount, 'u', 1)
    END AS input_amount,
    CASE WHEN 
        split_part(output_amount, 'u', 1) = 'false' THEN NULL
        WHEN output_amount ilike 'ciphertext%' THEN NULL
        WHEN split_part(output_amount, 'u', 1) NOT ilike '%field' THEN split_part(output_amount, 'u', 1) :: number
        ELSE NULL
    END AS output_amount,
    CASE WHEN 
        pool_id ilike 'ciphertext%' THEN NULL
        ELSE pool_id
    END AS pool_id,
    FROM 
        mapped_swaps
)
SELECT 
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    program_id,
    function,
    index,
    token_id,
    initiator_address,
    input_amount,
    output_amount,
    pool_id,
    CASE    
        WHEN initiator_address IS NULL THEN TRUE
        WHEN input_amount IS NULL THEN TRUE
        WHEN output_amount IS NULL THEN TRUE
        WHEN pool_id IS NULL THEN TRUE
        ELSE FALSE
    END AS is_private,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','INDEX']) }} AS swaps_arcane_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM cleaned