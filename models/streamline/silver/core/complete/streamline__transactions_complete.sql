{{ config (
    materialized = "incremental",
    incremental_strategy = 'merge',
    unique_key = "complete_transactions_id"
) }}

-- depends_on: {{ ref('bronze__transactions') }}

WITH transactions AS (
    SELECT
        VALUE:BLOCK_ID_REQUESTED :: INT AS block_id,
        DATA :id :: STRING AS transaction_id,
        {{ dbt_utils.generate_surrogate_key(
            ['VALUE:BLOCK_ID_REQUESTED :: INT', 'DATA :id :: STRING']
        ) }} AS complete_transactions_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        file_name,
        '{{ invocation_id }}' AS _invocation_id,
        ROW_NUMBER() OVER (PARTITION BY DATA :id ORDER BY inserted_timestamp DESC) AS rn
    FROM
    {% if is_incremental() %}
    {{ ref('bronze__transactions') }}
    {% else %}
    {{ ref('bronze__transactions_FR') }}
    {% endif %}

    {% if is_incremental() %}
    WHERE inserted_timestamp >= (
        SELECT MAX(modified_timestamp)
        FROM {{ this }}
    )
    {% endif %}
)

SELECT
    block_id,
    transaction_id,
    complete_transactions_id,
    inserted_timestamp,
    modified_timestamp,
    file_name,
    _invocation_id
FROM transactions
WHERE 
    rn = 1
    AND block_id IS NOT NULL