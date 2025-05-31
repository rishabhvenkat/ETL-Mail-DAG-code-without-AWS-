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

MAX_EMAILS_PER_FOLDER = 500  # Limit to avoid 504 errors

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
    def create_tables():
        """Create required tables and add missing columns if they don't exist"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Create email_group table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_group (
                email_id VARCHAR(255) PRIMARY KEY,
                conversation_id VARCHAR(255),
                subject TEXT,
                from_address VARCHAR(255),
                to_recipients JSONB,
                cc_recipients JSONB,
                received_datetime TIMESTAMP WITH TIME ZONE,
                sent_datetime TIMESTAMP WITH TIME ZONE,
                is_read BOOLEAN,
                has_attachments BOOLEAN,
                importance VARCHAR(50),
                folder VARCHAR(100),
                internet_message_id VARCHAR(255),
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Add missing columns to existing table if they don't exist
        try:
            cursor.execute("ALTER TABLE email_group ADD COLUMN IF NOT EXISTS conversation_id VARCHAR(255);")
            logging.info("Added conversation_id column to email_group table")
        except Exception as e:
            logging.info(f"conversation_id column handling: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE email_group ADD COLUMN IF NOT EXISTS from_address VARCHAR(255);")
            logging.info("Added from_address column to email_group table")
        except Exception as e:
            logging.info(f"from_address column handling: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE email_group ADD COLUMN IF NOT EXISTS folder VARCHAR(100);")
            logging.info("Added folder column to email_group table")
        except Exception as e:
            logging.info(f"folder column handling: {str(e)}")
        
        try:
            cursor.execute("ALTER TABLE email_group ADD COLUMN IF NOT EXISTS created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP;")
            logging.info("Added created_at column to email_group table")
        except Exception as e:
            logging.info(f"created_at column handling: {str(e)}")
        
        # Create email_relationships table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_relationships (
                email_id VARCHAR(255) PRIMARY KEY,
                conversation_id VARCHAR(255),
                is_response BOOLEAN,
                is_from_user BOOLEAN,
                original_email_id VARCHAR(255),
                response_time_hours NUMERIC,
                position_in_thread INTEGER,
                thread_length INTEGER,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create conversation_analytics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversation_analytics (
                conversation_id VARCHAR(255) PRIMARY KEY,
                total_emails INTEGER,
                user_emails_count INTEGER,
                external_emails_count INTEGER,
                user_response_count INTEGER,
                response_rate_percent NUMERIC(5,2),
                conversation_start_date DATE,
                last_activity_date DATE,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Create indexes for better performance (with error handling)
        try:
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_group_conversation_id ON email_group(conversation_id);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_group_folder ON email_group(folder);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_group_received_datetime ON email_group(received_datetime);")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_email_relationships_conversation_id ON email_relationships(conversation_id);")
        except Exception as e:
            logging.warning(f"Index creation warning: {str(e)}")
        
        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Tables and schema migration completed successfully")

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
            while url and len(emails) < MAX_EMAILS_PER_FOLDER:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    emails.extend(data.get("value", []))
                    if len(emails) >= MAX_EMAILS_PER_FOLDER:
                        break
                    url = data.get("@odata.nextLink", None)
                else:
                    raise Exception(f"Error fetching emails: {response.status_code}, {response.text}")
            return emails[:MAX_EMAILS_PER_FOLDER]

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
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        emails = data['emails_by_folder']
        relationships = data['email_relationships']
        analytics = data['conversation_analytics']

        # Insert emails with proper error handling
        for folder, email_list in emails.items():
            for email in email_list:
                try:
                    cursor.execute("""
                        INSERT INTO email_group (email_id, conversation_id, subject, from_address, to_recipients, cc_recipients, received_datetime, sent_datetime, is_read, has_attachments, importance, folder, internet_message_id)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email_id) DO UPDATE SET
                            conversation_id = EXCLUDED.conversation_id,
                            subject = EXCLUDED.subject,
                            from_address = EXCLUDED.from_address,
                            to_recipients = EXCLUDED.to_recipients,
                            cc_recipients = EXCLUDED.cc_recipients,
                            received_datetime = EXCLUDED.received_datetime,
                            sent_datetime = EXCLUDED.sent_datetime,
                            is_read = EXCLUDED.is_read,
                            has_attachments = EXCLUDED.has_attachments,
                            importance = EXCLUDED.importance,
                            folder = EXCLUDED.folder,
                            internet_message_id = EXCLUDED.internet_message_id;
                    """, (
                        email.get('id'),
                        email.get('conversationId'),
                        email.get('subject'),
                        email.get('from', {}).get('emailAddress', {}).get('address', ''),
                        json.dumps([r['emailAddress']['address'] for r in email.get('toRecipients', [])]),
                        json.dumps([r['emailAddress']['address'] for r in email.get('ccRecipients', [])]),
                        email.get('receivedDateTime'),
                        email.get('sentDateTime'),
                        email.get('isRead'),
                        email.get('hasAttachments'),
                        email.get('importance'),
                        email.get('folder'),
                        email.get('internetMessageId')
                    ))
                except Exception as e:
                    logging.error(f"Error inserting email {email.get('id', 'unknown')}: {str(e)}")
                    continue

        # Insert relationships
        for rel in relationships:
            try:
                cursor.execute("""
                    INSERT INTO email_relationships (email_id, conversation_id, is_response, is_from_user, original_email_id, response_time_hours, position_in_thread, thread_length)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (email_id) DO UPDATE SET
                        conversation_id = EXCLUDED.conversation_id,
                        is_response = EXCLUDED.is_response,
                        is_from_user = EXCLUDED.is_from_user,
                        original_email_id = EXCLUDED.original_email_id,
                        response_time_hours = EXCLUDED.response_time_hours,
                        position_in_thread = EXCLUDED.position_in_thread,
                        thread_length = EXCLUDED.thread_length;
                """, (
                    rel['email_id'], rel['conversation_id'], rel['is_response'], rel['is_from_user'],
                    rel['original_email_id'], rel['response_time_hours'], rel['position_in_thread'], rel['thread_length']
                ))
            except Exception as e:
                logging.error(f"Error inserting relationship for email {rel.get('email_id', 'unknown')}: {str(e)}")
                continue

        # Insert analytics with upsert
        for metric in analytics:
            try:
                cursor.execute("""
                    INSERT INTO conversation_analytics (conversation_id, total_emails, user_emails_count, external_emails_count, user_response_count, response_rate_percent, conversation_start_date, last_activity_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (conversation_id) DO UPDATE SET
                        total_emails = EXCLUDED.total_emails,
                        user_emails_count = EXCLUDED.user_emails_count,
                        external_emails_count = EXCLUDED.external_emails_count,
                        user_response_count = EXCLUDED.user_response_count,
                        response_rate_percent = EXCLUDED.response_rate_percent,
                        conversation_start_date = EXCLUDED.conversation_start_date,
                        last_activity_date = EXCLUDED.last_activity_date,
                        updated_at = CURRENT_TIMESTAMP;
                """, (
                    metric['conversation_id'], metric['total_emails'], metric['user_emails_count'],
                    metric['external_emails_count'], metric['user_response_count'],
                    metric['response_rate_percent'], metric['conversation_start_date'], metric['last_activity_date']
                ))
            except Exception as e:
                logging.error(f"Error inserting analytics for conversation {metric.get('conversation_id', 'unknown')}: {str(e)}")
                continue

        conn.commit()
        cursor.close()
        conn.close()
        logging.info("Data loaded to PostgreSQL successfully")

    # Define task dependencies
    tables = create_tables()
    token = get_graph_token()
    raw_data = extract_emails_from_folders(token)
    analytics = analyze_email_responses(raw_data)
    load_data = load_to_postgres(analytics)
    
    # Set up dependencies
    tables >> token >> raw_data >> analytics >> load_data
