
{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}
SELECT
    DATE_TRUNC('hour', block_timestamp) AS block_timestamp_hour,
    COUNT(
        DISTINCT tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN tx_succeeded THEN tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT tx_succeeded THEN tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT fee_payer
    ) AS unique_from_count,
    SUM(fee) AS total_fees,
    MAX(inserted_timestamp) AS _inserted_timestamp,  
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_transactions') }} ts
WHERE
    DATE_TRUNC('hour', block_timestamp) < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )
GROUP BY
    1
