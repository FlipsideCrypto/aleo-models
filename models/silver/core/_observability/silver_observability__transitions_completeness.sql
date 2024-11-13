{{ config(
    materialized = 'incremental',
    full_refresh = false,
    tags = ['recent_test']
) }}

WITH rel_blocks AS (

    SELECT
        block_id,
        block_timestamp
    FROM
        {{ ref('core__fact_blocks') }}
    WHERE
        block_timestamp < DATEADD(
            HOUR,
            -12,
            SYSDATE()
        )

    {% if is_incremental() %}
    AND (
        block_timestamp >= DATEADD(
            HOUR,
            -72,(
                SELECT
                    MAX(
                        max_block_timestamp
                    )
                FROM
                    {{ this }}
            )
        )
        OR ({% if var('OBSERV_FULL_TEST') %}
            block_id >= 0
        {% else %}
            block_id >= (
        SELECT
            MIN(VALUE) - 1
        FROM
            (
        SELECT
            blocks_impacted_array
        FROM
            {{ this }}
            qualify ROW_NUMBER() over (
        ORDER BY
            test_timestamp DESC) = 1), LATERAL FLATTEN(input => blocks_impacted_array))
        {% endif %})
    )
    {% endif %}
),
bronze AS (
    SELECT
        A.block_id,
        b.block_timestamp,
        A.tx_id,
        A.transition_id
    FROM
        {{ ref('silver__transitions') }} A
        JOIN rel_blocks b
        ON A.block_id = b.block_id

    {% if is_incremental() %}
    WHERE
        A.inserted_timestamp >= CURRENT_DATE - 14
        OR {% if var('OBSERV_FULL_TEST') %}
            1 = 1
        {% else %}
            (
                SELECT
                    MIN(VALUE) - 1
                FROM
                    (
                        SELECT
                            blocks_impacted_array
                        FROM
                            {{ this }}
                            qualify ROW_NUMBER() over (
                                ORDER BY
                                    test_timestamp DESC
                            ) = 1
                    ),
                    LATERAL FLATTEN(
                        input => blocks_impacted_array
                    )
            ) IS NOT NULL
        {% endif %}
    {% endif %}

    qualify(ROW_NUMBER() over (PARTITION BY tx_id,transition_id
    ORDER BY
        succeeded DESC, A.block_id DESC, A.tx_id DESC, A.inserted_timestamp DESC)) = 1
),
bronze_count AS (
    SELECT
        block_id,
        tx_id,
        block_timestamp,
        COUNT(
            DISTINCT transition_id
        ) AS transition_count
    FROM
        bronze
    GROUP BY
        ALL
),
bronze_min AS (
    SELECT
        MIN(block_id) block_id
    FROM
        bronze
),
bronze_node AS (
    SELECT
        block_id,
        tx_id,
        block_timestamp,
        transition_count
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        block_timestamp BETWEEN (
            SELECT
                MIN(block_timestamp)
            FROM
                rel_blocks
        )
        AND (
            SELECT
                MAX(block_timestamp)
            FROM
                rel_blocks
        )
)
SELECT
    'transitions' AS test_name,
    MIN(
        A.block_id
    ) AS min_block,
    MAX(
        A.block_id
    ) AS max_block,
    MIN(
        A.block_timestamp
    ) AS min_block_timestamp,
    MAX(
        A.block_timestamp
    ) AS max_block_timestamp,
    COUNT(1) AS blocks_tested,
    SUM(
        CASE
            WHEN COALESCE(
                b.transition_count,
                0
            ) - A.transition_count <> 0 THEN 1
            ELSE 0
        END
    ) AS blocks_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN COALESCE(
                b.transition_count,
                0
            ) - A.transition_count <> 0 THEN A.block_id
        END
    ) within GROUP (
        ORDER BY
            A.block_id
    ) AS blocks_impacted_array,
    SUM(
        ABS(
            COALESCE(
                b.transition_count,
                0
            ) - A.transition_count
        )
    ) AS transitions_impacted_count,
    ARRAY_AGG(
        CASE
            WHEN COALESCE(
                b.transition_count,
                0
            ) - A.transition_count <> 0 THEN OBJECT_CONSTRUCT(
                'block',
                A.block_id,
                'block_timestamp',
                A.block_timestamp,
                'diff',
                COALESCE(
                    b.transition_count,
                    0
                ) - A.transition_count,
                'blockchain_num_transitions',
                A.transition_count,
                'bronze_num_transitions',
                COALESCE(
                    b.transition_count,
                    0
                )
            )
        END
    ) within GROUP(
        ORDER BY
            A.block_id
    ) AS test_failure_details,
    SYSDATE() AS test_timestamp
FROM
    bronze_node A
    JOIN bronze_min bm
    ON A.block_id >= bm.block_id
    LEFT JOIN bronze_count b
    ON A.block_id = b.block_id
    AND A.tx_id = b.tx_id