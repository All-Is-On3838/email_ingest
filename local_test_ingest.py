### Program to extract dropoff data to CSV File
### Written by: Allison Li
### Date: June 1 2023
### Change Log:
###

from datetime import datetime, date
import os
from dotenv import load_dotenv

import msal
import requests 

from google.cloud import bigquery
import pandas as pd
import pandas_gbq


# load enviroment variables
load_dotenv()
SERVICEACCOUNT = os.getenv('SERVICE_ACCOUNT')
CLIENT_ID = os.getenv('CLIENTID')
CLIENT_SECRET = os.getenv('SECRET')
AUTHORITY = os.getenv('AUTHORITY')

# variable configuration
GRAPH_API_ENDPOINT = "https://graph.microsoft.com/v1.0"
SCOPES = ["https://graph.microsoft.com/.default"]
USER_ID = "data_services@lts.com"
SENDER_ADDRESS = "ALi@lts.com"
CURRENT_DATE = "2023-06-06" #date.today().isoformat()
FILE_LOCATION = os.getcwd()
TABLE_ID = "testandgo-352003.fuzzy_matching.dropoff_temp_copy"


def convert_filetime(dateString, str_format, desired_format):
    date_time_obj = datetime.strptime(dateString, str_format)
    return date_time_obj.strftime(desired_format)

def get_access_token():
    app = msal.ConfidentialClientApplication(
    client_id=CLIENT_ID,
    client_credential=CLIENT_SECRET,
    authority=AUTHORITY)

    scopes = SCOPES
    result = None
    result = app.acquire_token_silent(scopes, account=None)

    if not result:
        print("No suitable token exists in cache. Let's get a new one from Azure Active Directory.")
        result = app.acquire_token_for_client(scopes=scopes)

    if "access_token" in result:
        print("Access token is " + result["access_token"])

    return result

def get_message_id(new_headers):
    endpoint = f'{GRAPH_API_ENDPOINT}/users/{USER_ID}/messages?'
    try:
        response = requests.get(
            endpoint,
            headers=new_headers
        )

        if response.ok:
            print('Retrieved emails successfully')
            data = response.json()
            # print(data)
            for email in data['value']:
                # print(f"email from {email['sender']['emailAddress']['address']} has attachments: {email['hasAttachments']} id: {email['id']}")
                if email['sender']['emailAddress']['address'] == SENDER_ADDRESS:
                    message_id = email['id']
                    print("found email. id: ", message_id)
                    recieved_date = convert_filetime(email['receivedDateTime'], "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d")
                    print("email recieved: ", recieved_date)
                    if recieved_date == CURRENT_DATE:
                        print("correct email found")
                        return message_id
                    
                ##put in failsafe for case: multiple emails in same day
        else:
            print("issue getting message id")
            print (response.json())
        
    except Exception as e:
        print("issue calling graph API for messages")
        print(e)


def download_email_attachments(message_id, save_folder, new_headers):
    try:
        response = requests.get(
            f"{GRAPH_API_ENDPOINT}/users/{USER_ID}/messages/{message_id}/attachments",
            headers=new_headers
        )

        if response.ok:
            try:
                attachment_items = response.json()['value']
                for attachment in attachment_items:
                    file_name = attachment['name']
                    attachment_id = attachment['id']
                    attachment_content = requests.get(
                        f"{GRAPH_API_ENDPOINT}/users/{USER_ID}/messages/{message_id}/attachments/{attachment_id}/$value",
                        headers=new_headers
                    )
                    print(f"Saving file {file_name}...")
                    file_path = os.path.join(save_folder, file_name)
                    with open(file_path, 'wb') as _f:
                        _f.write(attachment_content.content)
                    return file_path
            except Exception as e:
                print("issue getting attachment files")
                print(e)
        else:
            print("issue getting attachments")
            print(response.json())
    except Exception as e:
        print("issue calling graph API for attachments")
        print(e)

def upload_dataframe_to_bigquery(dataframe):
    table_id = TABLE_ID
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICEACCOUNT
    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField('tracking_id', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('site_id', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('scan_time', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('address', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('isManualEntry', bigquery.enums.SqlTypeNames.STRING),
            bigquery.SchemaField('sample_id', bigquery.enums.SqlTypeNames.STRING)
        ],
        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        dataframe, 
        table_id, 
        job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

if __name__ == '__main__':
    # Step 1: get access token
    result = get_access_token()
    HEADERS = {
        'Authorization': 'Bearer ' + result['access_token']
    }

    # Step 2: get email message id
    if "access_token" in result:
        message_id = get_message_id(HEADERS)
        # Step 3: get attachment file
        file_path = download_email_attachments(message_id, FILE_LOCATION, HEADERS)
    else:
        print(result.get("error"))
        print(result.get("error_description"))
        print(result.get("correlation_id"))

   # Step 4: clean up file for upload
    try:
        df = pd.read_excel(file_path)
    except Exception as e:
        print("issue reading file with pandas")
        print(e)

    #remove Pickup, only keep StopeType Delivery
    try:
        rows_to_drop = []
        for i,r in df.iterrows():
            if str(df.loc[i,'StopeType']) == 'Pickup':
                rows_to_drop.append(i)
        df = df.drop(index=rows_to_drop)
    except Exception as e:
        print("issue removing pickup rows")
        print(e)

    #remove unneeded columns
    try:
        cols_to_keep = ['Parcel Barcode', 'Name', 'CompletedTime', 'Address', 'ManualScan']
        df = df.loc[:, cols_to_keep]
    except Exception as e:
        print("issue removing unneeded columns")
        print(e)

    #match tracking ids with sample ids in one row
    try:
        df['sample_id'] = ''
    except Exception as e:
        print("issue modifying sample_id")
        print(e)

    #fix datetime format
    try:
        df['CompletedTime'] = pd.to_datetime(df['CompletedTime']).dt.strftime('%Y-%m-%d %H:%M')
    except Exception as e:
        print("issue fixing datetime format")
        print(e)

    #rename columns
    try:
        df.rename(columns={
                'Parcel Barcode': 'tracking_id', 
                'Name': 'site_id', 
                'CompletedTime': 'scan_time', 
                'Address': 'address', 
                'ManualScan': 'isManualEntry'
                }, inplace=True)   
    except Exception as e:
        print("issue renaming columns")
        print(e)

    #reorder columns
    try:
        df = df[['tracking_id', 'sample_id', 'site_id', 'scan_time', 'address', 'isManualEntry']]
    except Exception as e:
        print("issue reordering columns")
        print(e)

    #change datatypes
    try:
        data_types_dict = { 
                        'tracking_id': 'string',
                        'sample_id': 'string',
                        'site_id': 'string',
                        'scan_time': 'string',
                        'address': 'string',
                        'isManualEntry': 'string'
                        }
        df = df.astype(data_types_dict)
        # print(df.dtypes)
    except Exception as e:
        print("issue changing datatypes")
        print(e)

    #upload dataframe to bigquery
    upload_dataframe_to_bigquery(df)

    # Step 5: clean up
    try:
        print("cleaning up downloaded files at...", file_path)
        os.remove(file_path)
    except Exception as e:
        print("issue deleting downloaded files")
        print(e)