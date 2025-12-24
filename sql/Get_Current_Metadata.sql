/* ============================================================
   Querying the snapshot SCD Type 2 for dbt. 
   This query applies to the program metadata.
   Developed by: Emerick Aguilera Gonzalez
   ============================================================ */
SELECT
    program_id,
    program_title,
    genre,
    program_duration_minutes
FROM dim_program_metadata
WHERE is_current = TRUE; 
