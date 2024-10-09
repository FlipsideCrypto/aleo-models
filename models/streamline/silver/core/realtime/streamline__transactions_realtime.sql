{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"transactions",
        "sql_limit" :"500",
        "producer_batch_size" :"100",
        "worker_batch_size" :"100",
        "sql_source" :"{{this.identifier}}" }
    ),
    enabled = false
) }}
-- depends_on: {{ ref('streamline__transactions_complete') }}
WITH blocks AS (

    SELECT
        block_id,
        block_timestamp,
        transactions,
        tx_count
    FROM
        {{ ref("silver__blocks") }}
    WHERE
        tx_count > 0
),
transaction_ids AS (
    SELECT
        b.block_id,
        b.block_timestamp,
        t.value :transaction :id :: STRING AS transaction_id
    FROM
        blocks b,
        TABLE(FLATTEN(PARSE_JSON(transactions))) t
    WHERE
        t.value :transaction :id IS NOT NULL),
        tx_to_pull AS (
            SELECT
                A.*
            FROM
                transaction_ids A
                LEFT JOIN {{ ref('streamline_transactions_complete') }}
        ) (
            SELECT
                block_id,
                block_timestamp,
                transaction_id,
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
        ) b
    SELECT
        ROUND(
            block_id,
            -4
        ) :: INT AS partition_key,
        block_timestamp,
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
