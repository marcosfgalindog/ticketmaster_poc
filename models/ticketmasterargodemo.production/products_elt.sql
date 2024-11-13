-- Jinja object to stablish an incremental table based on a primary key

{{
    config(
        materialized = 'incremental'
    )
}}


select 
  ev.id event_id
  , un.*
  , ev.db_stamp
from {{source("ticketmasterargodemo.stage","events_tb")}} ev
-- This statement allows you to access nested objects inside of a 'table'
CROSS JOIN UNNEST(ev.products) as un

-- This is the statment that will validate if the incremental is valid,
-- otherwise a full insert will be made 
{% if is_incremental() %}

LEFT JOIN {{source("ticketmasterargodemo.production","products_elt")}} th on 
    ev.id = th.event_id
WHERE th.event_id IS NULL

{% endif %}
