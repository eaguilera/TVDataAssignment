{{ 
  config(
    materialized='incremental',
    unique_key='program_sk'
  ) 
}}

/*
 *  dim_program_metadata.sql
 *  Implementing the program metadata as an SCD Type 2 Dimension
 *
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */

WITH staged AS (

    SELECT *
    FROM {{ ref('stg_program_metadata') }}

),

-- Detect changes using hash diff (common SCD2 pattern)
deduplicated AS (

    SELECT
        program_id,
        program_title,
        genre,
        program_duration_minutes,
        snapshot_date                                  AS valid_from,

        MD5(
            CONCAT_WS(
                '|',
                program_title,
                genre,
                program_duration_minutes
            )
        ) AS row_hash

    FROM staged

),

scd2_rows AS (

    SELECT
        *,
        LEAD(valid_from) OVER (
            PARTITION BY program_id
            ORDER BY valid_from
        ) AS next_valid_from

    FROM deduplicated

),

final AS (

    SELECT
        {{ dbt_utils.generate_surrogate_key(['program_id', 'valid_from']) }}
            AS program_sk,

        program_id,
        program_title,
        genre,
        program_duration_minutes,

        valid_from,

        COALESCE(
            next_valid_from - INTERVAL '1 DAY',
            DATE '9999-12-31'
        )                                               AS valid_to,

        CASE
            WHEN next_valid_from IS NULL THEN TRUE
            ELSE FALSE
        END                                             AS is_current

    FROM scd2_rows

)

SELECT *
FROM final

{% if is_incremental() %}
WHERE valid_from > (SELECT MAX(valid_from) FROM {{ this }})
{% endif %} 
