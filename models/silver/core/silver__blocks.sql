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
        -- IN FINAL
        DATA :header AS header,
        -- IN FINAL
        header :metadata :height :: INT AS block_id,
        -- IN FINAL
        header :metadata :timestamp :: datetime AS block_timestamp,
        DATA :block_hash :: STRING AS block_hash,
        DATA :previous_hash :: STRING AS previous_hash,
        -- IN FINAL
        COALESCE(ARRAY_SIZE(DATA :transactions) :: NUMBER, 0) AS tx_count,
        -- IN FINAL
        header :metadata :network AS network_id,
        header :metadata :coinbase_target :: bigint AS coinbase_target,
        header :metadata :cumulative_proof_target :: bigint AS cumulative_proof_target,
        header :metadata :cumulative_weight :: bigint AS cumulative_weight,
        -- IN FINAL
        header :metadata :round :: INT AS ROUND,
        -- use to identify address of block producer (validator) -- IN FINAL
        object_keys(
            DATA :authority :subdag :subdag
        ) AS rounds,
        -- IN FINAL, REPLACES PROPOSER ADDRESS
        {# DATA :transactions AS transactions,
        -- IN FINAL
        DATA :ratifications AS block_rewards,
        -- puzzle rewards (provers) and staker rewards (block reward). puzzle rewards are split by weight -- IN FINAL
        DATA :solutions :solutions :solutions AS puzzle_solutions -- target is the proportion of prover rewards #}
    FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
WHERE
    inserted_timestamp >= '{{ max_mod }}'
{% else %}
    {{ ref('bronze__blocks_FR') }}
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
