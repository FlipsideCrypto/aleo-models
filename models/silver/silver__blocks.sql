{{ config(
    materialized = 'incremental',
    unique_key = "block_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['modified_timestamp::DATE'],
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        DATA, -- IN FINAL
        DATA :header AS header, -- IN FINAL
        header :metadata :height :: INT AS block_id, -- IN FINAL
        header :metadata :timestamp :: datetime AS block_timestamp,
        DATA :block_hash :: STRING AS block_hash, -- IN FINAL
        COALESCE(ARRAY_SIZE(DATA :transactions) :: NUMBER, 0) AS tx_count, -- IN FINAL
        header :metadata :network as network_id, -- IN FINAL
        header :metadata :round as proving_round, -- use to identify address of block producer (validator) -- IN FINAL
        OBJECT_KEYS(DATA :authority :subdag :subdag) as prover_rounds, -- IN FINAL, REPLACES PROPOSER ADDRESS
        DATA :transactions as transactions, -- IN FINAL
        DATA :ratifications as block_rewards, -- puzzle rewards (provers) and staker rewards (block reward). puzzle rewards are split by weight -- IN FINAL
        DATA :solutions :solutions :solutions as puzzle_solutions -- target is the proportion of prover rewards
    FROM

    {% if is_incremental() %}   
        {{ ref('bronze__blocks') }}
    WHERE
        inserted_timestamp >= DATEADD(
            MINUTE,
            -5,(
                SELECT
                    MAX(
                        modified_timestamp
                    )
                FROM
                    {{ this }}
            )
        )
    {% else %}
        {{ ref('bronze__blocks_FR') }}
    {% endif %}
   
    qualify(
        ROW_NUMBER() over (
            PARTITION BY network_id, block_id
            ORDER BY block_id DESC, inserted_timestamp DESC
        )
    ) = 1
)
SELECT
    block_id,
    block_timestamp,
    network_id,
    tx_count,
    proving_round,
    prover_rounds,
    transactions,
    block_rewards,
    puzzle_solutions,
    DATA,
    {{ dbt_utils.generate_surrogate_key(
        ['network_id','block_id']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
