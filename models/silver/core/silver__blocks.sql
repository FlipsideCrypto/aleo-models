{{ config(
    materialized = 'incremental',
    unique_key = "blocks_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}
-- depends_on: {{ ref('bronze__blocks') }}
{% if execute %}

{% if is_incremental() %}
{% set max_mod_query %}

SELECT
    DATEADD(
        'minute',
        -5,
        MAX(
            modified_timestamp
        )
    )
FROM
    {{ this }}

    {% endset %}
    {% set max_mod = run_query(max_mod_query) [0] [0] %}
{% endif %}
{% endif %}

WITH base AS (
    SELECT
        DATA,
        DATA :header AS header,
        header :metadata :height :: INT AS block_id,
        header :metadata :timestamp :: datetime AS block_timestamp,
        DATA :block_hash :: STRING AS block_hash,
        DATA :previous_hash :: STRING AS previous_hash,
        COALESCE(ARRAY_SIZE(DATA :transactions) :: NUMBER, 0) AS tx_count,
        header :metadata :network AS network_id,
        header :metadata :coinbase_target :: bigint AS coinbase_target,
        header :metadata :cumulative_proof_target :: bigint AS cumulative_proof_target,
        header :metadata :cumulative_weight :: bigint AS cumulative_weight,
        header :metadata :round :: INT AS ROUND,
        object_keys(
            DATA :authority :subdag :subdag
        ) AS rounds
    FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
{% else %}
    {{ ref('bronze__blocks_FR') }}
{% endif %}
WHERE
    block_id IS NOT NULL

{% if is_incremental() %}
AND inserted_timestamp >= '{{ max_mod }}'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY network_id, block_id
ORDER BY
    inserted_timestamp DESC)) = 1
)
SELECT
    block_id,
    block_timestamp,
    network_id,
    tx_count,
    block_hash,
    previous_hash,
    ROUND,
    rounds,
    coinbase_target,
    cumulative_proof_target,
    cumulative_weight,
    CASE
        WHEN DATA :ratifications [0] :type = 'block_reward' THEN DATA :ratifications [0] :amount :: bigint
        WHEN DATA :ratifications [1] :type = 'block_reward' THEN DATA :ratifications [1] :amount :: bigint
    END block_reward,
    CASE
        WHEN DATA :ratifications [0] :type = 'puzzle_reward' THEN DATA :ratifications [0] :amount :: bigint
        WHEN DATA :ratifications [1] :type = 'puzzle_reward' THEN DATA :ratifications [1] :amount :: bigint
    END puzzle_reward,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
