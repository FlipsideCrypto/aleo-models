
{{ config(
    materialized = 'view',
    tags = ['noncore','recent_test']
) }}
/* run incremental timestamp value first then use it as a static value */
{% if execute %}

{% if is_incremental() %}
{% set query %}

SELECT
    MIN(DATE_TRUNC('hour', block_timestamp)) block_timestamp_hour
FROM
    {{ ref('silver__transitions') }}
WHERE
    inserted_timestamp >= (
        SELECT
            MAX(inserted_timestamp)
        FROM
            {{ this }}
    ) {% endset %}
    {% set min_block_timestamp_hour = run_query(query).columns [0].values() [0] %}
{% endif %}
{% endif %}

SELECT
    DATE_TRUNC('hour', ts.block_timestamp) AS block_timestamp_hour,
    MIN(block_id) AS block_id_min,
    MAX(block_id) AS block_id_max,
    COUNT(
        DISTINCT block_id
    ) AS block_count,
    COUNT(
        DISTINCT tx_id
    ) AS transaction_count,
    COUNT(
        DISTINCT CASE
            WHEN succeeded THEN tx_id
        END
    ) AS transaction_count_success,
    COUNT(
        DISTINCT CASE
            WHEN NOT succeeded THEN tx_id
        END
    ) AS transaction_count_failed,
    COUNT(
        DISTINCT ts.fee_payer
    ) AS unique_from_count,
    SUM(ts.fee) AS total_fees,
    MAX(ts.inserted_timestamp) AS _inserted_timestamp,  
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_hourly_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id

FROM
    {{ ref('silver__transitions_fee') }} ts
JOIN
    {{ref ('silver__native_transfers')}} tf
    USING(tx_id)
WHERE
    DATE_TRUNC('hour', ts.block_timestamp) < DATE_TRUNC(
        'hour',
        CURRENT_TIMESTAMP
    )
{% if is_incremental() %}
AND DATE_TRUNC(
    'hour',
    ts.block_timestamp
) >= '{{ min_block_timestamp_hour }}'
{% endif %}
GROUP BY
    1
