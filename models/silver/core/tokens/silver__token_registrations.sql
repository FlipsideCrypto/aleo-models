{{ config(
    materialized = 'table',
    tags = ['core', 'full_test']
) }}

with base_data as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        INPUTS
    from aleo_dev.core.fact_transitions
    where program_id = 'token_registry.aleo' 
    and function = 'register_token'
    and succeeded
),

flattened_inputs as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        value:id::string as id,
        value:value::string as value,
        row_number() over (partition by tx_id order by index) - 1 as input_index
    from base_data,
    lateral flatten(input => INPUTS)
),

parsed_inputs as (
    select 
        tx_id,
        block_id,
        block_timestamp,
        max(case when input_index = 0 then value end) as token_id_raw,
        max(case when input_index = 1 then value end) as name_raw,
        max(case when input_index = 2 then value end) as symbol_raw,
        max(case when input_index = 3 then value end) as decimals_raw,
        max(case when input_index = 4 then value end) as max_supply_raw,
        max(case when input_index = 5 then value end) as external_auth_required_raw,
        max(case when input_index = 6 then value end) as external_auth_party
    from flattened_inputs
    where block_id != 186732
    group by tx_id, block_id, block_timestamp

    union all

    select 
        tx_id,
        block_id,
        block_timestamp,
        max(case when input_index = 1 then value end) as token_id_raw,
        max(case when input_index = 3 then value end) as name_raw,
        max(case when input_index = 5 then value end) as symbol_raw,
        max(case when input_index = 6 then value end) as decimals_raw,
        max(case when input_index = 8 then value end) as max_supply_raw,
        max(case when input_index = 10 then value end) as external_auth_required_raw,
        max(case when input_index = 12 then value end) as external_auth_party
    from flattened_inputs
    where block_id = 186732 -- exceptional pondo token from early mainnet
    group by tx_id, block_id, block_timestamp
),
cleaned_strings as (
    select
        tx_id,
        block_id,
        block_timestamp,
        split_part(token_id_raw, 'field', 1) as token_id,
        split_part(name_raw, 'u', 1) as name_encoded,
        split_part(symbol_raw, 'u', 1) as symbol_encoded,
        split_part(decimals_raw, 'u', 1) as decimals,
        split_part(max_supply_raw, 'u', 1) as max_supply,
        external_auth_required_raw :: boolean as external_auth_required,
        external_auth_party
    from
        parsed_inputs
) 
select 
    tx_id,
    block_id,
    block_timestamp,
    token_id,
    udf_hex_to_string(substr(utils.udf_int_to_hex(name_encoded), 3)) as token_name,
    udf_hex_to_string(substr(udf_int_to_hex(symbol_encoded), 3)) as symbol,
    decimals,
    max_supply,
    external_auth_required,
    external_auth_party,
    name_encoded,
    symbol_encoded,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_id', 'token_name']
    ) }} AS tokens_id,
    SYSDATE() as inserted_timestamp,
    SYSDATE() as modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
from cleaned_strings
