{{ config(
    materialized = 'incremental',
    incremental_predicates = ['DBT_INTERNAL_DEST.block_timestamp::DATE >= (select min(block_timestamp::DATE) from ' ~ generate_tmp_view_name(this) ~ ')'],
    unique_key = ['fact_transactions_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_id,fee_msg,execution_msg,deployment_msg,owner_msg,finalize_msg,rejected_msg);",
    tags = ['core', 'full_test']
) }}

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
    {% set max_bd_query %}
SELECT
    MIN(
        block_timestamp :: DATE
    ) bd
FROM
    (
        SELECT
            block_timestamp
        FROM
            {{ ref('silver__transactions') }}
        WHERE
            modified_timestamp >= '{{ max_mod }}'
        UNION ALL
        SELECT
            block_timestamp
        FROM
            {{ ref('silver__transitions_fee') }}
        WHERE
            modified_timestamp >= '{{ max_mod }}'
    ) {% endset %}
    {% set max_bd = run_query(max_bd_query) [0] [0] %}
{% endif %}
{% endif %}
SELECT
    A.block_id,
    A.block_timestamp,
    A.tx_id,
    INDEX,
    CASE
        WHEN status = 'accepted' THEN TRUE
        ELSE FALSE
    END AS tx_succeeded,
    tx_type,
    fee_msg,
    execution_msg,
    deployment_msg,
    owner_msg,
    finalize_msg,
    rejected_msg,
    COALESCE(
        transition_count,
        0
    ) AS transition_count,
    COALESCE(
        fee_raw,
        0
    ) AS fee_raw,
    COALESCE(
        fee,
        0
    ) AS fee,
    b.fee_payer,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id']) }} AS fact_transactions_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('silver__transactions') }} A
    LEFT JOIN {{ ref('silver__transitions_fee') }}
    b
    ON A.tx_id = b.tx_id
    AND A.block_timestamp :: DATE = b.block_timestamp :: DATE

{% if is_incremental() %}
WHERE
    A.block_timestamp :: DATE >= '{{ max_bd }}'
    AND A.block_timestamp :: DATE >= '{{ max_bd }}'
    AND (
        A.modified_timestamp >= '{{ max_mod }}'
        OR b.modified_timestamp >= '{{ max_mod }}'
    )
{% endif %}
