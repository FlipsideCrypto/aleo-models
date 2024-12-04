{{ config(
    materialized='incremental',
    unique_key='liquidity_pool_actions_arcane_id',
    incremental_strategy='merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE'],
    tags=['noncore', 'full_test']
) }}

-- depends on: {{ ref('core__fact_transitions') }}
with 
root_actions as (
    select
        tx_id,
        program_id,
        function,
        program_id || '/' || function as root_action,
        CASE 
            WHEN function ILIKE '%add%' THEN 'Add'
            WHEN function ILIKE '%remove%' THEN 'Remove'
            ELSE 'OTHER'
        END as liquidity_action
    from
        aleo.core.fact_transitions
    where
        program_id ilike 'arcn%'
        and function ilike '%liq%'
        and function not ilike '%credits%'
),
reports as (
    select
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        INPUTS
    from
        aleo.core.fact_transitions
    where
        program_id ilike 'arcn%'
        and function = 'report'
),
parsed as (
    select
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        root_action,
        liquidity_action,
        inputs[1] :value :: string as liquidity_provider,
        inputs[2] :value :: string as token1_id,
        inputs[3] :value :: string as token2_id,
        split_part(inputs[4] :value, 'u', 1) :: number as token1_amount,
        split_part(inputs[5] :value, 'u', 1) :: number as token2_amount
    from 
        root_actions
    join
        reports using(tx_id)
),
tokens as (
    select
        token_id,
        name_encoded,
        token_name,
        decimals
    from
        aleo_dev.silver.token_registrations
)
select 
    p.block_timestamp,
    p.block_id,
    p.tx_id,
    p.succeeded,
    p.root_action,
    p.liquidity_action,
    p.liquidity_provider,
    p.token1_id,
    p.token2_id,
    case
        when p.token1_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' then 'Aleo'
        else t1.token_name
    end as token1_name,
    case
        when p.token2_id = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' then 'Aleo'
        else t2.token_name
    end as token2_name,
    p.token1_amount / power(10, coalesce(t1.decimals, 6)) as token1_amount,
    p.token2_amount / power(10, coalesce(t2.decimals, 6)) as token2_amount,
    {{ dbt_utils.generate_surrogate_key(['p.tx_id','p.token1_id', 'p.token2_id']) }} AS liquidity_pool_actions_arcane_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from
    parsed p
left join {{ ref('silver__token_registrations') }} t1 on p.token1_id = t1.token_id
left join {{ ref('silver__token_registrations') }} t2 on p.token2_id = t2.token_id
qualify(ROW_NUMBER() over(PARTITION BY p.tx_id
ORDER BY
    root_action) = 1)