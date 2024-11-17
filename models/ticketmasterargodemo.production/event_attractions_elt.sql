-- Jinja object to stablish an incremental table based on a primary key

{{
    config(
        materialized = 'incremental'
    )
}}



SELECT
    ev.id event_id
    , un.* except(aliases,images,_links,upcomingEvents,externalLinks,url)
    , ev.db_stamp
FROM {{source("ticketmasterargodemo.stage","events_tb")}} ev
CROSS JOIN UNNEST(ev._embedded.attractions) as un

-- This is the statment that will validate if the incremental is valid,
-- otherwise a full insert will be made 
{% if is_incremental() %}

LEFT JOIN {{source("ticketmasterargodemo.production","event_attractions_elt")}} th on 
    ev.id = th.event_id


WHERE th.event_id IS NULL

{% endif %}