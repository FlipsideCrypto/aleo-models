{{ config(
    materialized = 'incremental',
    unique_key = "block_idk_id",
    incremental_strategy = 'merge',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    merge_exclude_columns = ["inserted_timestamp"],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','full_test']
) }}

SELECT
    block_id,
    block_timestamp,
    b.key AS ROUND,
    C.value :batch_header :batch_id :: STRING AS batch_id,
    C.value :batch_header :author :: STRING AS author,
    C.value :batch_header :committee_id :: STRING AS committee_id,
    C.value :batch_header :transmission_ids AS transmission_ids,
    {{ dbt_utils.generate_surrogate_key(
        ['block_id','batch_id']
    ) }} AS block_idk_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__blocks') }} A,
    LATERAL FLATTEN(
        DATA :authority :subdag :subdag
    ) b,
    LATERAL FLATTEN(b.value) C

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
