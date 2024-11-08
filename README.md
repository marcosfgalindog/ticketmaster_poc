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


## CURRENT PROGRESS
### Data extraction
- DONE
    - Extraction of data has been complited
        - Added security measures so no api-key would be published
        - Created service account connection to BigQuery so no constant authentication via web browser would be needed
        - Set up BigQuery env
        - Set up Visual Studio Code enviroment
        - Set up repository for pushing updates
        - Creation of Ticketmaster credentials
        - Creation on pipeline for extraction
            - Adapted to support multiple type of objects: venues and attractions
        - Creation of Stage databases for ingesting the data
        - Data formatting to BigQuery needs (new line json format)

- TO DO
    - Probably a mass download of specific attractions is needed
    - Probably a mass download of specific venues is needed
    - Alert system in case an issue arrises on the pipeline

### Transformation step
- TO DO
    - Create query for ingesting data from stage and place it in a tailored dataset to be used in DOMO
    - Validations on Data consistency
    - Validations

### Visualizations
- Incorporate data into dashboards


### Here is proof of the data already flowing in
<img width="1026" alt="Screenshot 2024-11-08 at 10 15 26 AM" src="https://github.com/user-attachments/assets/c7ef2a79-ac72-45ef-910a-0040b50026f4">


    
