{{ config(
    materialized = 'incremental',
    unique_key = 'swaps_arcane_id',
    incremental_strategy = 'merge',
    merge_exclude_columns = ['inserted_timestamp'],
    cluster_by = ['modified_timestamp::DATE', 'swapper', 'swap_from_name', 'swap_to_name'],
    tags = ['noncore', 'full_test']
) }}

WITH root_actions AS (
    SELECT
        tx_id,
        program_id || '/' || function AS root_action
    FROM
        {{ ref('core__fact_transitions') }}
    WHERE
        program_id ILIKE 'arcn%'
        AND function ILIKE '%swap%'
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tx_id ORDER BY index DESC) = 1
),

reports AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        REPLACE(inputs[0]:value, 'field', '') AS address_from,
        REPLACE(inputs[1]:value, 'field', '') AS address_to,
        inputs[2]:value AS swap_from,
        inputs[3]:value AS swap_to,
        REPLACE(inputs[4]:value, 'u128', '') AS amount_from,
        REPLACE(inputs[5]:value, 'u128', '') AS amount_to
    FROM
        aleo.core.fact_transitions
    WHERE
        program_id = 'arcn_compliance_v1.aleo'
        AND function = 'report'
    {% if is_incremental() %}
        AND modified_timestamp >= (
            SELECT
                MAX(modified_timestamp)
            FROM
                {{ this }}
        )
    {% endif %}
),

agg AS (
    SELECT
        block_timestamp,
        block_id,
        tx_id,
        succeeded,
        root_action,
        address_from,
        address_to,
        CASE 
            WHEN swap_from = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' THEN 'Aleo'
            ELSE swap_from
        END AS swap_from,
        CASE 
            WHEN swap_to = '3443843282313283355522573239085696902919850365217539366784739393210722344986field' THEN 'Aleo'
            ELSE swap_to
        END AS swap_to,
        amount_from::number AS amount_from,
        amount_to::number AS amount_to
    FROM
        reports
    JOIN
        root_actions USING(tx_id)
)

SELECT 
    a.block_timestamp,
    a.block_id,
    a.tx_id,
    a.succeeded,
    a.address_from AS swapper,
    a.amount_from / power(10, COALESCE(b.decimals, 6)) AS swap_from_amount,
    COALESCE(b.token_name, a.swap_from) AS swap_from_name,
    CASE
        WHEN b.token_id IS NULL THEN '3443843282313283355522573239085696902919850365217539366784739393210722344986field'
        ELSE b.token_id
    END AS swap_from_id,
    a.amount_to / power(10, COALESCE(c.decimals, 6)) AS swap_to_amount,
    COALESCE(c.token_name, a.swap_to) AS swap_to_name,
    CASE
        WHEN c.token_id IS NULL THEN '3443843282313283355522573239085696902919850365217539366784739393210722344986field'
        ELSE c.token_id
    END AS swap_to_id,
    root_action,
    {{ dbt_utils.generate_surrogate_key(['a.tx_id', 'b.token_id']) }} AS swaps_arcane_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    agg a
LEFT JOIN 
    {{ ref('silver__token_registrations') }} b ON a.swap_from = b.token_id
LEFT JOIN 
    {{ ref('silver__token_registrations') }} c ON a.swap_to = c.token_id