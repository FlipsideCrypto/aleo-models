{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"10000",
        "producer_batch_size" :"1000",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}" }
    )
) }}

WITH blocks AS (

    SELECT
        block_id,
        transactions,
        tx_count
    FROM
        {{ref("silver__blocks")}}
    WHERE
        tx_count > 0
),
transaction_ids AS (
    SELECT
        b.block_id,
        t.value:transaction:id::STRING AS transaction_id
    FROM
        blocks b,
        TABLE(FLATTEN(PARSE_JSON(transactions))) t
    WHERE
        t.value:transaction:id IS NOT NULL
)
SELECT
    ROUND(
        block_id,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/transaction/' || transaction_id,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/dev/aleo/mainnet'
    ) AS request,
    block_id AS block_id_requested
FROM
    transaction_ids
ORDER BY
    block_id
