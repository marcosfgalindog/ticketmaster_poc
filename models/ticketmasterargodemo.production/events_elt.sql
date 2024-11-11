{{ config(
    materialized="table"
) }}

SELECT *
FROM {{source("ticketmasterargodemo.stage","events_tb")}}