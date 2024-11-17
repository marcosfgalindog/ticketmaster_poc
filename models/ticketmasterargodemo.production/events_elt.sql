-- Jinja object to stablish an incremental table based on a primary key

{{
    config(
        materialized = 'incremental'
    )
}}


SELECT 

    ev.id event_id
  , ev.type
  , ev.name
  , ev.description
  , ev.info
  , ev.locale
  , ev.pleaseNote
  , ev.dates.initialStartDate.dateTime start_datetime
  , ev.dates.end.dateTime end_datetime
  , ev.dates.status
  , ev.doorsTimes.localDate
  , ev.db_stamp

FROM {{source("ticketmasterargodemo.stage","events_tb")}} ev

-- This is the statment that will validate if the incremental is valid,
-- otherwise a full insert will be made 
{% if is_incremental() %}

LEFT JOIN {{source("ticketmasterargodemo.production","events_elt")}} th on 
    ev.id = th.event_id


WHERE th.event_id IS NULL

{% endif %}