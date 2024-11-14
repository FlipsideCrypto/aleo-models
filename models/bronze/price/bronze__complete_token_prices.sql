{{ config (
    materialized = 'view'
) }}

SELECT
    HOUR,
    'aleo' AS token_address,
    asset_id,
    symbol,
    'Aleo' AS NAME,
    decimals,
    price,
    'aleo' AS blockchain,
    'aleo' AS blockchain_name,
    blockchain_id,
    is_imputed,
    is_deprecated,
    provider,
    source,
    _inserted_timestamp,
    inserted_timestamp,
    modified_timestamp,
    complete_token_prices_id,
    _invocation_id
FROM
    {{ source(
        'crosschain_silver',
        'complete_token_prices'
    ) }}
WHERE
    asset_id = 'aleo' qualify(ROW_NUMBER() over(PARTITION BY asset_id
ORDER BY
    is_deprecated, blockchain_id) = 1)
