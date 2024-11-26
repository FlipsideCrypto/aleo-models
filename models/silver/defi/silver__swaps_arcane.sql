{{ config(
    materialized='incremental',
    unique_key='swaps_arcane_id',
    incremental_strategy='merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE', 'input_token_id', 'output_token_id'],
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
        aleo.core.fact_transitions
    WHERE
        succeeded
        AND program_id IN (
            'arcn_credits_in_helper_v2_2_2.aleo',
            'arcn_credits_in_helper_v2_2_3.aleo',
            'arcn_credits_out_helper_v2_2_2.aleo',
            'arcn_credits_out_helper_v2_2_3.aleo',
            'arcn_pool_v2_2_2.aleo',
            'arcn_priv_v2_2_2.aleo',
            'arcn_priv_v2_2_3.aleo',
            'arcn_pub_v2_2_2.aleo',
            'arcn_pub_v2_2_3.aleo',
            'arcn_puc_in_helper_v2_2_3.aleo',
            'arcn_puc_in_helper_v2_2_4.aleo',
            'arcn_puc_out_helper_v2_2_2.aleo',
            'arcn_puc_out_helper_v2_2_3.aleo'
        )
        AND function IN (
            'swap_amm',
            'swap_amm_credits_in',
            'swap_amm_credits_out'
        )
),
logic AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        program_id,
        function,
        index,
        CASE 
            -- Credits/PUC In Helpers
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo', 
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' THEN json_inputs[0]:value::string
            -- Credits/PUC Out Helpers
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' THEN json_inputs[0]:value::string
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[0]:value::string
        END AS pool_id,
        -- Initiator Address (recipient)
        CASE 
            -- Credits/PUC In Helpers
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo', 
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' THEN json_inputs[1]:value::string
            -- Credits/PUC Out Helpers
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' THEN json_inputs[1]:value::string
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[1]:value::string
        END AS initiator_address,

        -- Input Amount
        CASE 
            -- Credits/PUC In Helpers
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo',
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' THEN json_inputs[3]:value::string
            -- Credits/PUC Out Helpers
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' THEN json_inputs[4]:value::string
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[3]:value::string
        END AS input_amount,

        -- Input Token ID
        CASE 
            -- Credits/PUC In Helpers (always credits token)
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo',
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' 
            THEN '3443843282313283355522573239085696902919850365217539366784739393210722344986field'
            -- Credits/PUC Out Helpers
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' THEN json_inputs[2]:value::string
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[2]:value::string
        END AS input_token_id,

        -- Output Amount (min output amount)
        CASE 
            -- Credits/PUC In Helpers
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo',
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' THEN json_inputs[6]:value::string
            -- Credits/PUC Out Helpers
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' THEN json_inputs[5]:value::string
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[5]:value::string
        END AS output_amount,

        -- Output Token ID
        CASE 
            -- Credits/PUC In Helpers
            WHEN program_id IN (
                'arcn_credits_in_helper_v2_2_2.aleo',
                'arcn_credits_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_3.aleo',
                'arcn_puc_in_helper_v2_2_4.aleo'
            ) AND function = 'swap_amm_credits_in' THEN json_inputs[4]:value::string
            -- Credits/PUC Out Helpers (always credits token)
            WHEN program_id IN (
                'arcn_credits_out_helper_v2_2_2.aleo',
                'arcn_credits_out_helper_v2_2_3.aleo',
                'arcn_puc_out_helper_v2_2_2.aleo',
                'arcn_puc_out_helper_v2_2_3.aleo'
            ) AND function = 'swap_amm_credits_out' 
            THEN '3443843282313283355522573239085696902919850365217539366784739393210722344986field'
            -- Core Pool/Public/Private
            WHEN program_id IN (
                'arcn_pool_v2_2_2.aleo',
                'arcn_pub_v2_2_2.aleo',
                'arcn_pub_v2_2_3.aleo',
                'arcn_priv_v2_2_2.aleo',
                'arcn_priv_v2_2_3.aleo'
            ) AND function = 'swap_amm' THEN json_inputs[4]:value::string
        END AS output_token_id

    FROM parsed_data
),
cleaned as (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        program_id,
        function,
        index,
        pool_id,
        case when initiator_address ilike 'aleo%' then initiator_address
        else null
        end as initiator_address,
        case when input_amount ilike '%field' then null
        when input_amount ilike 'ciphertext%' then null
        else split_part(input_amount, 'u', 1) :: number
        end as input_amount,
        case when input_token_id ilike 'ciphertext%' then null
        else input_token_id
        end as input_token_id,
        case when output_amount ilike '%field' then null
        when output_amount ilike 'ciphertext%' then null
        else split_part(output_amount, 'u', 1) :: number
        end as output_amount,
        case when output_token_id = 'false' then null
        when output_token_id ilike 'ciphertext%' then null
        else output_token_id
        end as output_token_id,
    from
        logic
),
conditional_values AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        program_id,
        function,
        index,
        pool_id,
        initiator_address,
        -- Create conditional columns
        CASE WHEN program_id = 'arcn_pool_v2_2_2.aleo' THEN input_amount ELSE NULL END AS pool_input_amount,
        CASE WHEN program_id != 'arcn_pool_v2_2_2.aleo' THEN input_amount ELSE NULL END AS helper_input_amount,
        CASE WHEN input_token_id != '3443843282313283355522573239085696902919850365217539366784739393210722344986field' 
            THEN input_token_id ELSE NULL END AS non_credits_input_token,
        CASE WHEN input_token_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' 
            THEN input_token_id ELSE NULL END AS credits_input_token,
        CASE WHEN program_id = 'arcn_pool_v2_2_2.aleo' THEN output_amount ELSE NULL END AS pool_output_amount,
        CASE WHEN program_id != 'arcn_pool_v2_2_2.aleo' THEN output_amount ELSE NULL END AS helper_output_amount,
        CASE WHEN output_token_id != '3443843282313283355522573239085696902919850365217539366784739393210722344986field' 
            THEN output_token_id ELSE NULL END AS non_credits_output_token,
        CASE WHEN output_token_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' 
            THEN output_token_id ELSE NULL END AS credits_output_token
    FROM cleaned
),
aggregated_swaps AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        ARRAY_AGG(DISTINCT program_id) AS involved_programs,
        ARRAY_AGG(DISTINCT function) AS involved_functions,
        MIN(index) AS first_index,
        MAX(index) AS last_index,
        MAX(pool_id) AS pool_id,
        MAX(initiator_address) AS initiator_address,
        COALESCE(MAX(pool_input_amount), MAX(helper_input_amount)) AS input_amount,
        COALESCE(MAX(non_credits_input_token), MAX(credits_input_token)) AS input_token_id,
        COALESCE(MAX(pool_output_amount), MAX(helper_output_amount)) AS output_amount,
        COALESCE(MAX(non_credits_output_token), MAX(credits_output_token)) AS output_token_id
    FROM conditional_values
    GROUP BY block_timestamp, block_id, tx_id
)
SELECT 
    block_timestamp,
    block_id,
    tx_id,
    pool_id,
    initiator_address,
    input_amount,
    input_token_id,
    output_amount,
    output_token_id,
    involved_programs,
    involved_functions,
    first_index,
    last_index,
    CASE 
        -- Program-based privacy
        WHEN ARRAY_CONTAINS('arcn_priv_v2_2_2.aleo'::variant, involved_programs) 
            OR ARRAY_CONTAINS('arcn_priv_v2_2_3.aleo'::variant, involved_programs) THEN TRUE
        -- Data-based privacy indicators
        WHEN initiator_address IS NULL THEN TRUE
        WHEN input_amount IS NULL AND tx_id NOT LIKE 'au%' THEN TRUE  -- Exclude unrelated nulls
        WHEN output_amount IS NULL AND tx_id NOT LIKE 'au%' THEN TRUE
        WHEN input_token_id LIKE 'ciphertext%' THEN TRUE
        WHEN output_token_id LIKE 'ciphertext%' THEN TRUE
        -- If all key fields are present and no privacy indicators, then public
        WHEN initiator_address IS NOT NULL 
            AND input_amount IS NOT NULL 
            AND output_amount IS NOT NULL 
            AND input_token_id IS NOT NULL 
            AND output_token_id IS NOT NULL THEN FALSE
        -- Default to private if uncertain
        ELSE TRUE
    END AS is_private,
    {{ dbt_utils.generate_surrogate_key(['TX_ID','initiator_address']) }} AS swaps_arcane_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM aggregated_swaps