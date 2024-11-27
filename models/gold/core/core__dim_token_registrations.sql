{{ config(
    materialized = 'incremental',
    unique_key = ['dim_token_registrations_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core','full_test']
) }}

SELECT
    tx_id,
    block_id,
    block_timestamp,
    token_id,
    decode_u128_to_ascii(name_encoded) as token_name,
    decode_u128_to_ascii(symbol_encoded) as symbol,
    decimals,
    max_supply,
    external_auth_required,
    external_auth_party,
    name_encoded,
    symbol_encoded,
    {{ dbt_utils.generate_surrogate_key(
        ['token_id']
    ) }} AS dim_token_registrations_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM
    {{ ref('silver__token_registrations') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(modified_timestamp)
        FROM {{ this }}
    )
{% endif %}