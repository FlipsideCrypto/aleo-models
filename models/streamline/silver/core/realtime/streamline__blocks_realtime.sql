{{ config (
    materialized = "view",
    post_hook = fsc_utils.if_data_call_function_v2(
        func = 'streamline.udf_bulk_rest_api_v2',
        target = "{{this.schema}}.{{this.identifier}}",
        params ={ "external_table" :"blocks",
        "sql_limit" :"20000",
        "producer_batch_size" :"5000",
        "worker_batch_size" :"1000",
        "sql_source" :"{{this.identifier}}",
        "order_by_column": "block_number" }
    )
) }}
-- depends_on: {{ ref('streamline__blocks_complete') }}
WITH blocks AS (

    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number
    FROM
        {{ ref("streamline__blocks_complete") }}
)
SELECT
    ROUND(
        block_number,
        -4
    ) :: INT AS partition_key,
    {{ target.database }}.live.udf_api(
        'GET',
        '{Service}/block/' || block_number,
        OBJECT_CONSTRUCT(
            'Content-Type',
            'application/json'
        ),{},
        'Vault/dev/aleo/mainnet'
    ) AS request,
    block_number
FROM
    blocks
