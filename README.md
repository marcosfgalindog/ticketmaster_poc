# ticketmaster_reportery
Ticketmaster Demo for reporting

## The project will be divided in 4 parts
- INGESTION
    - The ingestion process will run on a cronjob hosted locally, this will be executed at least once a day. 
    - This process will live locally. A version that can be hosted online with the necessary security precau.
    - ETL 
        - The data will be placed in Snowflake.
        - Given the data structure, multiple tables will be created.

- ELT process
    - This will be hosted in Snowflake with the help of DBT.
    - DBT will provide a lineage of the data and versioning as well. 
    - DBT will host a standardized dataset
    - Desired: to connect a repo to the current version of DBT

- Data Analysis
    - Analyze the data in order to have Marketing metrics
    - Check feasibility of creating a forecast of the sales
        - dont invest too much on this
        
- Reportery (depends on the data pulled)
    - Need to get an idea of how to use DOMO
    - Create a simple dashboard
    - Provide metric with most recent information pulled.