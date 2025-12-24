
{{ config(materialized='view') }}


/*
 *  Loading of the Program Metadata
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */
WITH source AS (

    SELECT
        program_id,
        program_title,
        genre,
        CAST(program_duration_minutes AS INT)      AS program_duration_minutes,
        CAST(snapshot_date AS DATE)                AS snapshot_date
    FROM {{ source('raw', 'program_metadata') }}

)

SELECT *
FROM source 
