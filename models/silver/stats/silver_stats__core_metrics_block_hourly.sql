{{ config(
    materialized = 'view',
    tags = ['noncore']
) }}

SELECT
    DATE_TRUNC(
        'hour',
        block_timestamp
    ) AS block_timestamp_hour,
    MIN(block_id) AS block_id_min,
    MAX(block_id) AS block_id_max,
    COUNT(
        1
    ) AS block_count,
    {{ dbt_utils.generate_surrogate_key(
        ['block_timestamp_hour']
    ) }} AS core_metrics_block_hourly_id,
    MAX(inserted_timestamp) AS inserted_timestamp,
    MAX(modified_timestamp) AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_blocks') }}
WHERE
    block_timestamp_hour < DATE_TRUNC('hour', SYSDATE())
GROUP BY
    1
