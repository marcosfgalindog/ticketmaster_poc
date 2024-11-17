-- Jinja object to stablish an incremental table based on a primary key

{{
    config(
        materialized = 'incremental'
    )
}}


-- after testing the relationship between attraction and product is irrelevant

SELECT 
  ev.id event_id
--   , un.id attraction_id
  , pr.*
  , ev.db_stamp
FROM {{source("ticketmasterargodemo.stage","events_tb")}} ev
-- CROSS JOIN UNNEST(ev._embedded.attractions) un
CROSS JOIN UNNEST(ev.products) pr

-- This is the statment that will validate if the incremental is valid,
-- otherwise a full insert will be made 
{% if is_incremental() %}

LEFT JOIN {{source("ticketmasterargodemo.production","products_elt")}} th on 
    ev.id = th.event_id
WHERE th.event_id IS NULL

{% endif %}
