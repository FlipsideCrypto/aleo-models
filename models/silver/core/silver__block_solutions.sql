{{ config(
    materialized = 'incremental',
    unique_key = "block_solutions_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        puzzle_reward AS block_puzzle_reward,
        b.value :partial_solution :address :: STRING AS address,
        b.value :partial_solution :counter :: STRING AS counter,
        b.value :partial_solution :epoch_hash :: STRING AS epoch_hash,
        b.value :partial_solution :solution_id :: STRING AS solution_id,
        b.value :target :: bigint AS target,
        SUM(target) over(
            PARTITION BY block_id
        ) AS total_target,
        target / total_target AS pct
    FROM
        {{ ref('silver__blocks') }} A,
        LATERAL FLATTEN(
            DATA :solutions :solutions :solutions
        ) b

{% if is_incremental() %}
WHERE
    modified_timestamp >= DATEADD(
        MINUTE,
        -5,(
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    )
{% endif %}
)
SELECT
    block_id,
    block_timestamp,
    block_puzzle_reward,
    address,
    counter,
    epoch_hash,
    solution_id,
    target,
    pct * block_puzzle_reward AS reward_raw,
    reward_raw / pow(
        10,
        6
    ) AS reward,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','solution_id']
    ) }} AS block_solutions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
