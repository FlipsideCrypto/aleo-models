{{ config(
    materialized = 'incremental',
    unique_key = ['dim_labels_id'],
    incremental_strategy = 'merge',
    cluster_by = 'modified_timestamp::DATE',
    merge_exclude_columns = ['inserted_timestamp'],
    tags = ['core']
) }}

SELECT
    'aleo' AS blockchain,
    creator,
    address,
    address_name,
    label_type,
    label_subtype,
    project_name,
    {{ dbt_utils.generate_surrogate_key(
        [' address ']
    ) }} AS dim_labels_id,
    SYSDATE() AS inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver__labels') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(
                modified_timestamp
            )
        FROM
            {{ this }}
    )
{% endif %}