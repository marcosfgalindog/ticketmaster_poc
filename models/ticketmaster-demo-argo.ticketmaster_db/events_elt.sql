SELECT *
FROM {{source("ticketmaster-demo-argo.stage_db","events_tb")}}