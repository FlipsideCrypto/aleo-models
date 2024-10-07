{{ config(
    materialized = 'table',
    unique_key = "program_id",
    tags = ['core','full_test']
) }}

WITH base as (
    SELECT
        program_id,
        program,
        mappings
    FROM {{ ref('bronze__programs') }}
)
SELECT
    program_id,
    program :data :: STRING AS program,
    mappings :data :: STRING AS mappings,
    {{ dbt_utils.generate_surrogate_key(
        ['program_id']
    ) }} AS complete_program_id,
    SYSDATE() AS insert_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS invocation_id
FROM 
    base