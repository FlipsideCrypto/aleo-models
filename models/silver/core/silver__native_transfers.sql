{{ config(
    materialized = 'incremental',
    incremental_predicates = ["COALESCE(DBT_INTERNAL_DEST.block_timestamp::DATE,'2099-12-31') >= (select min(block_timestamp::DATE) from " ~ generate_tmp_view_name(this) ~ ")"],
    unique_key = ['transition_id'],
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core', 'recent_test']
) }}

WITH base AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        transition_id,
        outputs,
        function,
        succeeded,
        inserted_timestamp,
        modified_timestamp,
        invocation_id
    FROM
        {{ ref('silver__transitions') }}
    WHERE
        program_id = 'credits.aleo'
        AND function IN (
            'transfer_public',
            'transfer_private',
            'transfer_public_as_signer',
            'transfer_private_to_public',
            'transfer_public_to_private'
        )
),
output_args AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        transition_id,
        function,
        succeeded,
        REGEXP_SUBSTR(
            outputs[array_size(outputs)-1] :value :: STRING,
            'arguments:\\s*\\[(.*?)\\]',
            1,
            1,
            'sie'
        ) as args_string,
        inserted_timestamp,
        modified_timestamp,
        invocation_id
    FROM
        base
),
output_args_cleaned AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        transition_id,
        function,
        succeeded,
        SPLIT(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    REGEXP_REPLACE(args_string, '\\s+', ''),
                    '\\[|\\]',
                    ''
                ),
                'u64$',
                ''
            ),
            ','
        ) as args_array,
        inserted_timestamp,
        modified_timestamp,
        invocation_id
    FROM output_args
),
mapped_transfers AS (
    SELECT
        block_id,
        block_timestamp,
        tx_id,
        transition_id,
        succeeded,
        function,
        CASE
            WHEN function IN ('transfer_public', 'transfer_public_as_signer') THEN args_array[0]
            WHEN function = 'transfer_private_to_public' THEN null
            WHEN function = 'transfer_public_to_private' THEN args_array[0]
            WHEN function = 'transfer_private' THEN null
        END :: STRING as transfer_from,
        CASE
            WHEN function IN ('transfer_public', 'transfer_public_as_signer') THEN args_array[1]
            WHEN function = 'transfer_private_to_public' THEN args_array[0]
            WHEN function = 'transfer_public_to_private' THEN null
            WHEN function = 'transfer_private' THEN null
        END :: STRING as transfer_to,
        CASE
            WHEN function IN ('transfer_public', 'transfer_public_as_signer') THEN args_array[2]
            WHEN function = 'transfer_private_to_public' THEN args_array[1]
            WHEN function = 'transfer_public_to_private' THEN args_array[1]
            WHEN function = 'transfer_private' THEN null
        END :: STRING as amount,
        inserted_timestamp,
        modified_timestamp,
        invocation_id
    FROM output_args_cleaned
)
select
    block_id,
    block_timestamp,
    tx_id,
    transition_id,
    succeeded as tx_succeeded,
    function as transfer_type,
    transfer_from as sender,
    transfer_to as receiver,
    REPLACE(amount, 'u64', '') :: INT as amount,
    'aleo_credits' as currency,
    inserted_timestamp,
    modified_timestamp,
    invocation_id
from 
    mapped_transfers
