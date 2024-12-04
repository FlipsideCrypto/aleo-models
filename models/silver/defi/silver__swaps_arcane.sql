{{ config(
    materialized='incremental',
    unique_key='swaps_arcane_id',
    incremental_strategy='merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE', 'swapper','swap_from_name', 'swap_to_name'],
    tags=['noncore', 'full_test']
) }}

-- depends on: {{ ref('core__fact_transitions') }}
with 
root_actions as (
    select
        tx_id,
        program_id || '/' || function as root_action
    from
        {{ ref('core__fact_transitions') }}
    where
        program_id ilike 'arcn%'
        and function ilike '%swap%'
    {% if is_incremental() %}
    and
        modified_timestamp >= (
            select
                MAX(
                    modified_timestamp
                )
            from
                {{ this }}
        )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY tx_id 
        ORDER BY index DESC
    ) = 1
),
reports as (
    select
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        replace(inputs[0] :value, 'field', '') as address_from,
        replace(inputs[1] :value, 'field', '') as address_to,
        replace(inputs[2] :value, 'field', '') as swap_from,
        replace(inputs[3] :value, 'field', '') as swap_to,
        replace(inputs[4] :value, 'u128', '') as amount_from,
        replace(inputs[5] :value, 'u128', '') as amount_to
    from
        aleo.core.fact_transitions
    where
        program_id = 'arcn_compliance_v1.aleo'
        and function = 'report'
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
agg as (
select
    block_timestamp,
    block_id,
    tx_id,
    succeeded,
    root_action,
    address_from,
    address_to,
    CASE 
        WHEN swap_from = '3443843282313283355522573239085696902919850365217539366784739393210722344986' THEN 'Aleo'
        ELSE swap_from
    END as swap_from,
    CASE 
        WHEN swap_to = '3443843282313283355522573239085696902919850365217539366784739393210722344986' THEN 'Aleo'
        ELSE swap_to
    END as swap_to,
    amount_from :: number as amount_from,
    amount_to :: number as amount_to
from
    reports
join
    root_actions using(tx_id)
)

select 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.succeeded,
    a.address_from as swapper,
    a.amount_from / power(10, coalesce(b.decimals, 6)) as swap_from_amount,
    coalesce(b.token_name, a.swap_from) as swap_from_name,
    case
        when b.token_id is null then '3443843282313283355522573239085696902919850365217539366784739393210722344986'
        else b.token_id
    end as swap_from_id,
    a.amount_to / power(10, coalesce(c.decimals, 6)) as swap_to_amount,
    coalesce(c.token_name, a.swap_to) as swap_to_name,
    case
        when c.token_id is null then '3443843282313283355522573239085696902919850365217539366784739393210722344986'
        else c.token_id
    end as swap_to_id,
    root_action,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id','b.token_id']) }} AS swaps_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from
    agg a
    left join {{ ref('silver__token_registrations') }} b on a.swap_from = b.token_id
    left join {{ ref('silver__token_registrations') }} c on a.swap_to = c.token_id