import pandas as pd
from azure.storage.blob import BlobServiceClient, ContentSettings
import requests
import smtplib
from email.mime.text import MIMEText
from email.utils import formatdate
from datetime import date
import os

# Environment Variables
EMAIL_PW = os.environ["email_pw"]
AZURE_ACCOUNT_KEY = os.environ["azure_account_key"]
AZURE_ACCOUNT_NAME = os.environ["azure_account_name"]
AZURE_BLOB_NAME = os.environ["azure_blob_name"]
AZURE_CONTAINER_NAME = os.environ["azure_container_name"]

def sendUnsuccessEmail(message):
    """
    Sends an email notification for unsuccessful operations.

    Parameters:
    - message (str): The error message to be included in the email.

    Returns:
    - None
    """
    from_address = "dummy_email@gmail.com"
    to_address = "dummy_email@gmail.com"
    subject = "Error in DLD Script"
    
    msg = MIMEText(message)
    msg['From'] = from_address
    msg['To'] = to_address
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    smtp_server = smtplib.SMTP('smtp.office365.com', 587)
    smtp_server.ehlo()
    smtp_server.starttls()
    smtp_server.ehlo()
    smtp_server.login(from_address, EMAIL_PW)
    smtp_server.sendmail(from_address, to_address, msg.as_string())
    smtp_server.close()

    print("Error Email sent successfully!")

def fetch_dld_transaction():
    """
    Fetches DLD transaction data from the open data gateway.

    Returns:
    - pd.DataFrame: DataFrame containing DLD transaction data.
    """
    try:
        print("Data fetching started")
        url = 'https://gateway.dubailand.gov.ae/open-data/transactions'
        headers = {'Content-Type': 'application/json'}

        started_date = "1/1/2023"
        today = date.today()
        today_date = today.strftime("%m/%d/%Y")

        payload = {
            'P_FROM_DATE': started_date,
            'P_TO_DATE': today_date,
            'P_GROUP_ID': '',
            'P_IS_OFFPLAN': '',
            'P_IS_FREE_HOLD': '',
            'P_AREA_ID': '',
            'P_USAGE_ID': '',
            'P_PROP_TYPE_ID': '',
            'P_TAKE': '10',
            'P_SKIP': '0',
            'P_SORT': 'TRANSACTION_NUMBER_ASC'
        }

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()
        total = data['response']['result'][0]['TOTAL']

        payload['P_TAKE'] = str(total)

        response = requests.post(url, headers=headers, json=payload)
        data = response.json()

        df = pd.DataFrame(data['response']['result'])

        selected_columns = [
            'TRANSACTION_NUMBER',
            'INSTANCE_DATE',
            'GROUP_EN',
            'PROCEDURE_EN',
            'IS_OFFPLAN_EN',
            'IS_FREE_HOLD_EN',
            'USAGE_EN',
            'AREA_EN',
            'PROP_TYPE_EN',
            'PROP_SB_TYPE_EN',
            'TRANS_VALUE',
            'PROCEDURE_AREA',
            'ACTUAL_AREA',
            'ROOMS_EN',
            'PARKING',
            'NEAREST_METRO_EN',
            'NEAREST_MALL_EN',
            'NEAREST_LANDMARK_EN',
            'TOTAL_BUYER',
            'TOTAL_SELLER',
            'MASTER_PROJECT_EN',
            'PROJECT_EN'
        ]
        df = df[selected_columns]
        df = df.rename(columns={
            'TRANSACTION_NUMBER': 'Transaction Number',
            'INSTANCE_DATE': 'Transaction Date',
            'GROUP_EN': 'Transaction Type',
            'PROCEDURE_EN': 'Transaction Sub Type',
            'IS_OFFPLAN_EN': 'Registration Type',
            'IS_FREE_HOLD_EN': 'Is Free Hold?',
            'USAGE_EN': 'Usage',
            'AREA_EN': 'Area',
            'PROP_TYPE_EN': 'Property Type',
            'PROP_SB_TYPE_EN': 'Property Sub Type',
            'TRANS_VALUE': 'Amount',
            'PROCEDURE_AREA': 'Transaction Size (sq.m)',
            'ACTUAL_AREA': 'Property Size (sq.m)',
            'ROOMS_EN': 'Room(s)',
            'PARKING': 'Parking',
            'NEAREST_METRO_EN': 'Nearest Metro',
            'NEAREST_MALL_EN': 'Nearest Mall',
            'NEAREST_LANDMARK_EN': 'Nearest Landmark',
            'TOTAL_BUYER': 'No. of Buyer',
            'TOTAL_SELLER': 'No. of Seller',
            'MASTER_PROJECT_EN': 'Master Project',
            'PROJECT_EN': 'Project'
        })

        if not df.empty:
            df.to_csv("results.csv")
            print("Data fetching finished")
            return df
        else:
            return None
    except Exception as e:
        print(f"Error in fetching DLD transactions data {e}")
        message = "Something went wrong in fetching DLD transaction data. Error is: " + str(e)
        sendUnsuccessEmail(message)
        pass

def update_blob(dld_df):
    """
    Uploads processed data to Azure Blob Storage.

    Parameters:
    - dld_df (pd.DataFrame): DataFrame containing DLD transaction data.

    Returns:
    - None
    """
    try:
        print("Uploading to blob started")
        your_account_name = AZURE_ACCOUNT_NAME
        your_account_key = AZURE_ACCOUNT_KEY
        connection_string = f"DefaultEndpointsProtocol=https;AccountName={your_account_name};AccountKey={your_account_key};EndpointSuffix=core.windows.net"

        updated_csv_data = dld_df.to_csv(index=False)

        blob_service_client = BlobServiceClient.from_connection_string(connection_string)

        container_name = AZURE_CONTAINER_NAME
        blob_name = AZURE_BLOB_NAME
        blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
        blob_client.upload_blob(updated_csv_data, overwrite=True, content_settings=ContentSettings(content_type='text/csv'))

        print("Uploading to blob completed")
    except Exception as e:
        print(f"Error in uploading to Azure Blob: {e}")
        message = "Something went wrong in uploading to Azure Blob, DLD transaction data. Error is: " + str(e)
        sendUnsuccessEmail(message)

# Fetch DLD transaction data
dld_data = fetch_dld_transaction()

# Check if the data is not empty and update Azure Blob Storage
if not dld_data.empty:
    update_blob(dld_data)
