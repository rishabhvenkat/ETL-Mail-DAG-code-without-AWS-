from airflow import DAG
from airflow.decorators import task
from airflow.sdk import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os
import logging
from typing import Dict, List, Any

POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ms_graph_email_etl_postgres_with_analytics',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['email', 'graph', 'etl', 'postgres', 'analytics']
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
        folder_names = json.loads(Variable.get("MS_GRAPH_FOLDER_NAMES", default='["Inbox", "Sent Items"]'))

        headers = {
            "Authorization": f"Bearer {access_token}",
            "Accept": "application/json"
        }

        def get_folder_id(folder_name):
            special_folders = {
                "inbox": "Inbox",
                "sent items": "SentItems"
            }
            lower_name = folder_name.strip().lower()
            if lower_name in special_folders:
                url = f"{graph_base}/users/{user_email}/mailFolders/{special_folders[lower_name]}"
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    return response.json()["id"]
                else:
                    raise Exception(f"Error fetching {folder_name}: {response.status_code}, {response.text}")

            # Search in child folders
            child_url = f"{graph_base}/users/{user_email}/mailFolders/Inbox/childFolders"
            response = requests.get(child_url, headers=headers)
            if response.status_code != 200:
                raise Exception(f"Error fetching child folders: {response.status_code}, {response.text}")

            for folder in response.json().get("value", []):
                if folder["displayName"].strip().lower() == lower_name:
                    return folder["id"]
            raise Exception(f"Folder '{folder_name}' not found.")

        def fetch_emails(folder_id):
            emails = []
            url = f"{graph_base}/users/{user_email}/mailFolders/{folder_id}/messages"
            url += "?$select=id,conversationId,subject,from,toRecipients,ccRecipients,receivedDateTime,sentDateTime,isRead,hasAttachments,importance,flag,internetMessageId"
            url += "&$orderby=receivedDateTime desc"
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

    @task()
    def analyze_email_responses(data):
        from datetime import datetime

        all_emails = data.get("emails_by_folder", {})
        user_email = data.get("user_email", "")

        all_email_list = []
        for folder, emails in all_emails.items():
            for email in emails:
                email['folder'] = folder
                all_email_list.append(email)

        conversations = {}
        for email in all_email_list:
            conv_id = email.get('conversationId')
            if conv_id:
                conversations.setdefault(conv_id, []).append(email)

        email_relationships = []
        conversation_analytics = []

        for conv_id, emails in conversations.items():
            emails_sorted = sorted(emails, key=lambda x: x.get('receivedDateTime', x.get('sentDateTime', '')))

            for i, email in enumerate(emails_sorted):
                from_email = email.get('from', {}).get('emailAddress', {}).get('address', '')
                is_from_user = from_email.lower() == user_email.lower()

                response_time = None
                original_email_id = None
                if i > 0:
                    original_email = emails_sorted[i - 1]
                    original_email_id = original_email.get('id')
                    try:
                        t1 = datetime.fromisoformat(email.get('receivedDateTime', '').replace('Z', '+00:00'))
                        t0 = datetime.fromisoformat(original_email.get('receivedDateTime', '').replace('Z', '+00:00'))
                        response_time = (t1 - t0).total_seconds() / 3600
                    except:
                        pass

                email_relationships.append({
                    'email_id': email.get('id'),
                    'conversation_id': conv_id,
                    'is_response': i > 0,
                    'is_from_user': is_from_user,
                    'original_email_id': original_email_id,
                    'response_time_hours': response_time,
                    'position_in_thread': i + 1,
                    'thread_length': len(emails_sorted)
                })

            user_emails = [e for e in emails_sorted if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() == user_email.lower()]
            external_emails = [e for e in emails_sorted if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() != user_email.lower()]

            user_response_count = 0
            external_requiring_response = 0

            for i, email in enumerate(emails_sorted[:-1]):
                from_email = email.get('from', {}).get('emailAddress', {}).get('address', '')
                if from_email.lower() != user_email.lower():
                    external_requiring_response += 1
                    next_email = emails_sorted[i + 1]
                    next_from = next_email.get('from', {}).get('emailAddress', {}).get('address', '')
                    if next_from.lower() == user_email.lower():
                        user_response_count += 1

            response_rate = (user_response_count / external_requiring_response * 100) if external_requiring_response > 0 else 0

            conversation_analytics.append({
                'conversation_id': conv_id,
                'total_emails': len(emails_sorted),
                'user_emails_count': len(user_emails),
                'external_emails_count': len(external_emails),
                'user_response_count': user_response_count,
                'response_rate_percent': round(response_rate, 2),
                'conversation_start_date': emails_sorted[0].get('receivedDateTime', '')[:10],
                'last_activity_date': emails_sorted[-1].get('receivedDateTime', '')[:10]
            })

        return {
            "emails_by_folder": all_emails,
            "user_email": user_email,
            "email_relationships": email_relationships,
            "conversation_analytics": conversation_analytics
        }

    @task()
    def load_to_postgres(data):
        pass  # Assume load logic is implemented below...

    token = get_graph_token()
    raw_data = extract_emails_from_folders(token)
    analytics = analyze_email_responses(raw_data)
    load_to_postgres(analytics)
