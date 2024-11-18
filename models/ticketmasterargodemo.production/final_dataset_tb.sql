{{
    config(
        materialized = 'table'
    )
}}

SELECT 
  ev.event_id
  , ev.name
  , ev.type ev_type_name
  , ev.start_datetime
  , ev.startDateTime
  , ev.endDateTime
  -- , ev.end_datetime
  , cl.genre.name class_gen
  , cl.subGenre.name class_subgen
  , cl.subType.name class_subtype
  , cl.type.name class_type_name
  , pr.min price_min
  , pr.max price_max
  , pr.type
  , pro.name product_name
  , pro.classifications[OFFSET(0)].genre.name product_genre_name
  , pro.classifications[OFFSET(0)].subGenre.name product_subgen_name
  , pro.classifications[OFFSET(0)].type.name product_type_name
  , ven.latitude
  , abs(ven.longitude)*(-1) longitude
  , CASE
    WHEN ven.latitude BETWEEN 25.0000 AND 49.0000
         AND longitude BETWEEN -125.0000 AND -67.0000 THEN 1
    ELSE 0
  END AS is_within_bounds
  , ven.state
  , ven.venue_name
  , ven.city.name city_name
  , ev.status.code
  , eae.name attraction_name
  , eae.classifications[OFFSET(0)].genre.name attraction_genre
  , eae.classifications[OFFSET(0)].subGenre.name attraction_subgenre
  , eae.classifications[OFFSET(0)].type.name attraction_type
  , eae.classifications[OFFSET(0)].subType.name attraction_subtype
  , ev.db_stamp
  , ROW_NUMBER() OVER (PARTITION BY ev.event_id order by ev.db_stamp) dummy_var
FROM {{ref("events_elt")}} ev
LEFT JOIN {{ref("classification_elt")}} cl
  ON ev.event_id = cl.event_id
LEFT JOIN {{ref("priceranges_elt")}} pr
  ON ev.event_id = pr.event_id
LEFT JOIN {{ref("products_elt")}} pro
  ON ev.event_id = pro.event_id
LEFT JOIN {{ref("venues_elt")}} ven
  ON ev.event_id = ven.event_id 
LEFT JOIN {{ref("event_attractions_elt")}} eae
  oN ev.event_id = eae.event_id
