{{ config(
    materialized = 'view',
    tags = ['core', 'full_test']
) }}

-- depends on {{ ref('core__fact_transitions') }}
with base_data as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        INPUTS
    from 
        {{ ref('core__fact_transitions') }}
    where 
        program_id = 'token_registry.aleo' 
        and function = 'register_token'
        and succeeded
    qualify
        row_number() over (partition by tx_id order by index) = 1
),

flattened_inputs as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        value:id::string as id,
        value:value::string as value,
        f.index
    from base_data,
    lateral flatten(input => INPUTS) f
),

parsed_inputs as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        OBJECT_AGG(index::STRING, value::variant) AS J,
        J:"0"::STRING as token_id_raw,
        J:"1"::STRING as name_raw,
        J:"2"::STRING as symbol_raw,
        J:"3"::STRING as decimals_raw,
        J:"4"::STRING as max_supply_raw,
        J:"5"::STRING as external_auth_required_raw,
        J:"6"::STRING as external_auth_party
    from 
        flattened_inputs 
    group by 
        tx_id, block_id, block_timestamp
),
cleaned_strings as (
    select
        tx_id,
        block_id,
        block_timestamp,
        token_id_raw as token_id,
        split_part(name_raw, 'u', 1) as name_encoded,
        split_part(symbol_raw, 'u', 1) as symbol_encoded,
        split_part(decimals_raw, 'u', 1) as decimals,
        split_part(max_supply_raw, 'u', 1) as max_supply,
        external_auth_required_raw :: boolean as external_auth_required,
        external_auth_party
    from
        parsed_inputs
        where len(symbol_encoded) <30
),
fin as (
    select 
        tx_id as tx_id_created,
        block_id as block_id_created,
        block_timestamp as block_timestamp_created,
        token_id,
        utils.udf_hex_to_string(substr(utils.udf_int_to_hex(name_encoded), 3)) as token_name,
        utils.udf_hex_to_string(substr(utils.udf_int_to_hex(symbol_encoded), 3)) as symbol,
        decimals,
        max_supply,
        external_auth_required,
        external_auth_party,
        name_encoded,
        symbol_encoded
    from 
        cleaned_strings
    
    UNION ALL

    SELECT
        null as tx_id_created,
        null as block_id_created,
        null as block_timestamp_created,
        '3443843282313283355522573239085696902919850365217539366784739393210722344986field' as token_id,
        'Aleo' as token_name,
        'ALEO' as symbol,
        6 as decimals,
        null as max_supply,
        FALSE as external_auth_required,
        null as external_auth_party,
        null as name_encoded,
        null as symbol_encoded
)
SELECT
    tx_id_created,
    block_id_created,
    block_timestamp_created,
    token_id,
    token_name,
    symbol,
    decimals :: int as decimals,
    max_supply as max_supply,
    external_auth_required,
    external_auth_party,
    name_encoded,
    symbol_encoded,
    {{ dbt_utils.generate_surrogate_key(
        ['token_id']
    ) }} AS tokens_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    fin