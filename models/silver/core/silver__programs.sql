{{ config(
    materialized = 'incremental',
    unique_key = "programs_id",
    incremental_strategy = 'merge',
    merge_exclude_columns = ["inserted_timestamp"],
    tags = ['core','full_test']
) }}

WITH base AS (

    SELECT
        block_id,
        block_timestamp,
        program_id,
        deployment_msg
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        deployment_msg IS NOT NULL

{% if is_incremental() %}
AND modified_timestamp >= DATEADD(
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
    block_id AS deployment_block_id,
    block_timestamp AS deployment_block_timestamp,
    program_id,
    deployment_msg :edition :: INT AS edition,
    deployment_msg :program :: STRING AS program,
    TRY_PARSE_JSON(
        deployment_msg :verifying_keys
    ) AS verifying_keys,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id','edition']
    ) }} AS programs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
