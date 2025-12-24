{{ config(materialized='table') }}

/*
 *  Implementing the Enriched Events and their Validations Reasons
 *  Developed by: 
 *               Eng. Emerick Aguilera Gonzalez
 */

SELECT *
FROM {{ ref('int_enriched_events') }}
WHERE cardinality(validation_reasons) = 0
