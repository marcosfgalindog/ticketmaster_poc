-- -- Jinja object to stablish an incremental table based on a primary key

-- {{
--     config(
--         materialized = 'incremental'
--     )
-- }}



-- SELECT
--   ev.id event_id
--   , ev._embedded.venues[OFFSET(0)].location.latitude
--   , ev._embedded.venues[OFFSET(0)].location.longitude
--   , ev._embedded.venues[OFFSET(0)].name venue_name
--   , ev._embedded.venues[OFFSET(0)].state.name state
--   , ev._embedded.venues[OFFSET(0)].city city
--   , ev._embedded.venues[OFFSET(0)].id venue_id
--   , ev._embedded.venues[OFFSET(0)].country.name country_name
--   , ev._embedded.venues[OFFSET(0)].type country_type
--   , ev.db_stamp
-- FROM `ticketmasterargodemo.stage.events_tb` ev
-- CROSS JOIN UNNEST(ev._embedded.venues) as un

-- -- This is the statment that will validate if the incremental is valid,
-- -- otherwise a full insert will be made 
-- {% if is_incremental() %}

-- LEFT JOIN {{source("ticketmasterargodemo.production","venues_elt")}} th on 
--     ev.id = th.event_id


-- WHERE th.event_id IS NULL

-- {% endif %}