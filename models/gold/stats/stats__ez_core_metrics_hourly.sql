{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_timestamp_hour",
    cluster_by = ['block_timestamp_hour::DATE'],
    tags = ['noncore','recent_test'],
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    }} }
) }}

SELECT
    block_timestamp_hour,
    block_id_min,
    block_id_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    unique_to_count,
    total_fees AS total_fees_native,
    core_metrics_hourly_id AS ez_core_metrics_hourly_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}