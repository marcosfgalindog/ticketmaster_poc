import os
import time
import json
import requests 
from datetime import datetime

# invoque dbt commands from the pythyon itself
from dbt.cli.main import dbtRunner, dbtRunnerResult


def save_request(request_data, page_number, file_path='events_data.json'):
# def save_request(request_data, page_number, file_path='events_data_231101-241101'):
    # Load existing requests if the file exists
    
    if os.path.exists('datasets/'+ file_path):
        with open('datasets/'+ file_path, 'r') as file:
            requests = json.load(file)
    else:
        requests = []
    
    # Add timestamp to each event 
    new_request = []
    tmp_timestamp = datetime.now().isoformat()


    # add a timestamp to every object being processed
    for tm_object in request_data:

        tm_object['db_stamp'] = tmp_timestamp
        requests.append(tm_object)
    
    
    # Save updated requests back to the file
    
    with open('datasets/' + file_path, 'w') as file:
        json.dump(requests, file, indent=4)

    print(page_number)

# pipeline to download information from an endpoint

# pipeline to download information from an endpoint

error_case = None
def ticketmaster_download_data(object_to_retrieve,endstart='',start= '2023-11-01T00:00:00Z',end ='2024-11-01T00:00:00Z',page_size = '80'):
    
    global error_case

    print(f'Object to extract: {object_to_retrieve}')
    #default values
    i = 0               # to start in the first page
    next_page = ''      # default page value

    consumer_key = open('api_key.txt','r').read()
    country_code = 'US'


    base_url = 'https://app.ticketmaster.com'

    url0 = f'https://app.ticketmaster.com/discovery/v2/{object_to_retrieve}.json?'
    url0 += 'countryCode=' + country_code
    if endstart != '':
        url0 += '&startEndDateTime='+endstart
    if start != '':
        # url0 += '&startDateTime=' + start 
        url0 += '&onsaleStartDateTime=' + start 
    if end != '':
        # url0 += '&endDateTime=' + end
        url0 += '&onsaleEndDateTime=' + end
    # url0 += '&classificationName=' + 'music'
    url0 += '&size=' + page_size + '&apikey=' + consumer_key

    total_pages = 1
    total_elements = 0

    while i < total_pages:

        # check if this is the first page to prepare
        if i == 0:
            
            # get the first batch of information and retrieve the amount of pages to process
            events_list = requests.request('GET', url0 )
            total_pages = events_list.json()['page']['totalPages']
            total_elements = events_list.json()['page']['totalElements']

            # save the data requested
            if total_pages==0:
                print('There are no elements to retrieve.')
                break
            save_request(
                events_list.json()['_embedded'][object_to_retrieve]
                , str(events_list.json()['page']['number'])
                , f'{object_to_retrieve}_data.json'
                )
            
            # increase the page
            i += 1
            print(f'Total pages: {total_pages}')
            print(f'Total enries: {total_elements}')
            if total_pages > 1000:
                # break
                print('it will break')

        else:
            # proceed in case there is a next page in the request data
            if events_list.json().get('_links',{}).get('next','') != '':
                # request the 'next' page in the link in case there are more data
                events_list = requests.request('GET', base_url + events_list.json()['_links']['next']['href']+ '&apikey=' + consumer_key)
                try:
                    error_case = events_list
                    save_request(
                        events_list.json()['_embedded'][object_to_retrieve]
                        , str(events_list.json()['page']['number'])
                        , f'{object_to_retrieve}_data.json'
                        )
                    i += 1 
                except Exception as e:
                    print(e)
            # stop downloading more 
            else:
                break
        
        # in order to prevent the api_key to be throttled
        time.sleep(1)

    # unit test for validating the downloaded data

    # load recently created json
    with open(f'datasets/{object_to_retrieve}_data.json', 'r') as file:
        full_data = json.load(file)

    if len(full_data) == total_elements:
        print(f'The download of the object {object_to_retrieve} was successful.')
        print(f'Total elements downloaded: {total_elements}')
    else:
        print('There was an issue in the pipeline')
        print('Here is the last request''s response ')
        print('VVVVVVVV')
        print(error_case.text)
        print('')
        print(f'Rows extracted: {len(full_data)}' )

    # the json file needs to be formatted in the proper formatting for GCP
    print('Prepare data to have BigQuery necessary formatting.')
    with open(f'datasets/{object_to_retrieve}_data_f.json', "w") as new_file:
        for row in full_data:
            new_file.write(json.dumps(row))
            new_file.write("\n")

    # delete unformatted version of the data
    os.remove(f'datasets/{object_to_retrieve}_data.json')


# function to upload raw data into BigQuery

from google.cloud import bigquery

# settup global variables for service-account connexion 

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'dbt_service_account_key.json'
client = bigquery.Client(project='ticketmasterargodemo')


def upload_data_to_bigquer(object_of_interest):
    global client

    filename = f'datasets/{object_of_interest}_data_f.json'
    dataset_id = 'stage'
    table_id = f'{object_of_interest}_tb'

    dataset_ref = client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    job_config.autodetect = True

    with open(filename, "rb") as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            # location="us-east4",  # This is region specific.
            location="us",  # This is a multiregion.
            job_config=job_config,
        )  # API request

    job.result()  # Waits for table load to complete.

    print("Loaded {} rows for object {} into {}:{}.".format(job.output_rows, object_of_interest, dataset_id, table_id))




def download_data():

    start_of_month = datetime.utcnow().replace(day=1).strftime('%Y-%m-%dT00:00:00Z')
    current_date = datetime.utcnow().strftime('%Y-%m-%d') + 'T00:00:00Z'

    ticketmaster_download_data('events',current_date,current_date,current_date,'50')
    print('')
    # ticketmaster_download_data('attractions',start_of_month,current_date,'80') # this is pending
    # print('')
    # ticketmaster_download_data('venues',start_of_month,current_date,'80') # got limited to only 1000 records per deep-page request


def db_stage_cleanup():

    for object in ['events']: #,'attractions','venues']:
        
        # 3 , for deleting the data
        query_string = f"""DROP TABLE `ticketmasterargodemo.stage.{object}_tb`;"""
        results = client.query_and_wait(query_string)

        print(f'The table {object} has been cleaned.')
        print('')


def dataset_upload():

    upload_data_to_bigquer('events')
    # upload_data_to_bigquer('attractions')
    # upload_data_to_bigquer('venues')
    print('')

def run_dbt_models():
    # execute dbt commands
    dbt = dbtRunner()
    cli_args = ["run","--select","events_elt.sql classification_elt.sql event_attractions_elt.sql priceranges_elt.sql products_elt.sql venues_elt.sql"]

    res: dbtRunnerResult = dbt.invoke(cli_args)

    print('')
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
    print('')

def run_final_dbt_dataset():
    # execute dbt commands
    dbt = dbtRunner()
    cli_args = ["run","--select","final_dataset_tb"]

    res: dbtRunnerResult = dbt.invoke(cli_args)

    print('')
    for r in res.result:
        print(f"{r.node.name}: {r.status}")
    print('')


def run_process():
    download_data()
    db_stage_cleanup()
    dataset_upload()
    run_dbt_models()
    run_final_dbt_dataset()

if __name__ == "__main__":
    run_process()