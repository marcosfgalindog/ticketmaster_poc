-- Jinja object to stablish an incremental table based on a primary key

{{
    config(
        materialized = 'incremental'
    )
}}


WITH attr_classifications as (

    SELECT 
    ev.id event_id
    , un.*
    , ev.db_stamp
    FROM {{source("ticketmasterargodemo.stage","events_tb")}} ev
    -- This statement allows you to access nested objects inside of a 'table'
    CROSS JOIN UNNEST(ev.classifications) as un
    WHERE un.primary = true
    qualify row_number() over(partition by ev.id order by ev.db_stamp desc) = 1 
)

SELECT ac.*
FROM attr_classifications ac

-- This is the statment that will validate if the incremental is valid,
-- otherwise a full insert will be made 
{% if is_incremental() %}

LEFT JOIN {{source("ticketmasterargodemo.production","classification_elt")}} th on 
    ac.event_id = th.event_id
WHERE th.event_id IS NULL

{% endif %}
