from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ms_graph_email_etl_postgres',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    tags=['email', 'graph', 'etl', 'postgres']
) as dag:

    @task()
    def get_graph_token():
        tenant_id = os.environ.get("MS_GRAPH_TENANT_ID")
        client_id = os.environ.get("MS_GRAPH_CLIENT_ID")
        client_secret = os.environ.get("MS_GRAPH_CLIENT_SECRET")

        url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
            "scope": "https://graph.microsoft.com/.default"
        }

        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            return response.json()['access_token']
        else:
            raise Exception(f"Failed to get token: {response.status_code} - {response.text}")

    @task()
    def extract_emails_from_folders(access_token: str):
        graph_base = "https://graph.microsoft.com/v1.0"
        user_email = Variable.get("MS_GRAPH_USER_EMAIL")
        folder_names = json.loads(Variable.get("MS_GRAPH_FOLDER_NAMES", default_var = '["Inbox"]'))

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

    def get_folder_id(folder_name):
        if folder_name.strip().lower() == "inbox":
            # Directly get the Inbox folder
            inbox_url = f"{graph_base}/users/{user_email}/mailFolders/Inbox"
            response = requests.get(inbox_url, headers=headers)
            if response.status_code == 200:
                folder = response.json()
                return folder["id"]
            else:
                raise Exception(f"Error fetching Inbox folder: {response.status_code}, {response.text}")
        else:
            # Search in child folders of Inbox
            inbox_url = f"{graph_base}/users/{user_email}/mailFolders/Inbox"
            response = requests.get(inbox_url + "/childFolders", headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error fetching child folders: {response.status_code}, {response.text}")
            folders = response.json().get("value", [])
            for folder in folders:
                if folder["displayName"].strip().lower() == folder_name.strip().lower():
                    return folder["id"]
            raise Exception(f"Folder '{folder_name}' not found.")


        def fetch_emails(folder_id):
            emails = []
            url = f"{graph_base}/users/{user_email}/mailFolders/{folder_id}/messages?$orderby=receivedDateTime desc"
            while url:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    emails.extend(data.get("value", []))
                    url = data.get("@odata.nextLink", None)
                else:
                    raise Exception(f"Error fetching emails: {response.status_code}, {response.text}")
            return emails

        all_emails = {}
        for folder_name in folder_names:
            folder_id = get_folder_id(folder_name)
            emails = fetch_emails(folder_id)
            all_emails[folder_name] = emails
        return {"emails_by_folder": all_emails, "user_email": user_email}
    
        logging.info(f"User: {user_email}")
        for folder, emails in all_emails.items():
            logging.info(f"Folder '{folder}': {len(emails)} emails")
            if emails:
                logging.info(f"Sample email from '{folder}': {json.dumps(emails[0], indent=2)}")

        return {"emails_by_folder": all_emails, "user_email": user_email}

    @task()

    def load_to_postgres(data):
        import logging

        if data is None:
            raise ValueError("No data passed to load_to_postgres")

        logging.info("Data received for loading: {data}")

        all_emails = data.get("emails_by_folder", {})

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        conn = pg_hook.get_conn()

        cursor = conn.cursor()

        cursor.execute("""

            CREATE TABLE IF NOT EXISTS email (

                email_id TEXT PRIMARY KEY,

                thread_id TEXT,

                from_name TEXT,

                from_email TEXT,

                subject TEXT,

                received_date DATE,

                received_time TIME,

                folder TEXT

            );

        """)

        for folder_name, emails in all_emails.items():

            for msg in emails:

                try:

                    cursor.execute("""

                        INSERT INTO email (

                            email_id, thread_id, from_name, from_email,

                            subject, received_date, received_time, folder

                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)

                        ON CONFLICT (email_id) DO NOTHING;

                    """, (

                        msg['id'],

                        msg.get('conversationId'),

                        msg.get('from', {}).get('emailAddress', {}).get('name'),

                        msg.get('from', {}).get('emailAddress', {}).get('address'),

                        msg.get('subject', ''),

                        msg.get('receivedDateTime', '')[:10],

                        msg.get('receivedDateTime', '')[11:19],

                        folder_name

                    ))

                except Exception as e:

                    print(f"[ERROR] Failed to insert email id {msg.get('id')}: {e}")

        conn.commit()

        cursor.close()
 

    # DAG task flow
    token = get_graph_token()
    email_data = extract_emails_from_folders(token)
    load_to_postgres(email_data)

    # DAG task flow
    token = get_graph_token()
    email_data = extract_emails_from_folders(token)
    load_to_postgres(email_data)
