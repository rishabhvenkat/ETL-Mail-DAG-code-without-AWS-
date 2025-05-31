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

        def fetch_all_emails(folder_id, folder_name):
            """Fetch all emails from a folder without limit"""
            emails = []
            url = f"{graph_base}/users/{user_email}/mailFolders/{folder_id}/messages"
            url += "?$select=id,conversationId,subject,from,toRecipients,ccRecipients,receivedDateTime,sentDateTime,isRead,hasAttachments,importance,flag,internetMessageId"
            url += "&$orderby=receivedDateTime desc"
            
            page_count = 0
            while url:
                page_count += 1
                logging.info(f"Fetching page {page_count} from folder '{folder_name}' (Current total: {len(emails)} emails)")
                
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()
                    page_emails = data.get("value", [])
                    emails.extend(page_emails)
                    
                    # Log progress every 10 pages
                    if page_count % 10 == 0:
                        logging.info(f"Processed {page_count} pages from folder '{folder_name}', fetched {len(emails)} emails so far")
                    
                    url = data.get("@odata.nextLink", None)
                    
                    # If no more pages, break
                    if not url:
                        break
                        
                else:
                    raise Exception(f"Error fetching emails from {folder_name} (page {page_count}): {response.status_code}, {response.text}")
            
            logging.info(f"Successfully fetched {len(emails)} emails from folder '{folder_name}' in {page_count} pages")
            return emails

        all_emails = {}
        total_emails_count = 0
        
        for folder_name in folder_names:
            logging.info(f"Starting extraction from folder: {folder_name}")
            folder_id = get_folder_id(folder_name)
            emails = fetch_all_emails(folder_id, folder_name)
            all_emails[folder_name] = emails
            total_emails_count += len(emails)
            logging.info(f"Completed extraction from folder '{folder_name}': {len(emails)} emails")

        logging.info(f"Total emails extracted from all folders: {total_emails_count}")
        return {"emails_by_folder": all_emails, "user_email": user_email, "total_count": total_emails_count}

    @task()
    def analyze_email_responses(data):
        from datetime import datetime

        all_emails = data.get("emails_by_folder", {})
        user_email = data.get("user_email", "")
        total_count = data.get("total_count", 0)

        logging.info(f"Starting analysis of {total_count} emails")

        all_email_list = []
        for folder, emails in all_emails.items():
            for email in emails:
                email['folder'] = folder
                all_email_list.append(email)

        logging.info(f"Analyzing {len(all_email_list)} emails across {len(all_emails)} folders")

        conversations = {}
        for email in all_email_list:
            conv_id = email.get('conversationId')
            if conv_id:
                conversations.setdefault(conv_id, []).append(email)

        logging.info(f"Found {len(conversations)} unique conversations")

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

        logging.info(f"Analysis completed: {len(email_relationships)} relationships, {len(conversation_analytics)} conversation metrics")

        return {
            "emails_by_folder": all_emails,
            "user_email": user_email,
            "email_relationships": email_relationships,
            "conversation_analytics": conversation_analytics,
            "total_count": total_count
        }

    @task()
    def load_to_postgres(data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        emails = data['emails_by_folder']
        relationships = data['email_relationships']
        analytics = data['conversation_analytics']
        total_count = data.get('total_count', 0)

        logging.info(f"Starting to load {total_count} emails to PostgreSQL")

        # Insert emails with proper error handling and batch processing
        email_count = 0
        error_count = 0
        
        for folder, email_list in emails.items():
            logging.info(f"Loading {len(email_list)} emails from folder '{folder}'")
            
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
                    email_count += 1
                    
                    # Log progress every 1000 emails
                    if email_count % 1000 == 0:
                        logging.info(f"Loaded {email_count} emails so far...")
                        
                except Exception as e:
                    logging.error(f"Error inserting email {email.get('id', 'unknown')}: {str(e)}")
                    error_count += 1
                    continue

        logging.info(f"Loaded {email_count} emails successfully, {error_count} errors")

        # Insert relationships
        relationship_count = 0
        relationship_errors = 0
        
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
                relationship_count += 1
            except Exception as e:
                logging.error(f"Error inserting relationship for email {rel.get('email_id', 'unknown')}: {str(e)}")
                relationship_errors += 1
                continue

        logging.info(f"Loaded {relationship_count} relationships successfully, {relationship_errors} errors")

        # Insert analytics with upsert
        analytics_count = 0
        analytics_errors = 0
        
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
                analytics_count += 1
            except Exception as e:
                logging.error(f"Error inserting analytics for conversation {metric.get('conversation_id', 'unknown')}: {str(e)}")
                analytics_errors += 1
                continue

        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Data loading completed successfully:")
        logging.info(f"  - Emails: {email_count} loaded, {error_count} errors")
        logging.info(f"  - Relationships: {relationship_count} loaded, {relationship_errors} errors")
        logging.info(f"  - Analytics: {analytics_count} loaded, {analytics_errors} errors")

    # Define task dependencies
    tables = create_tables()
    token = get_graph_token()
    raw_data = extract_emails_from_folders(token)
    analytics = analyze_email_responses(raw_data)
    load_data = load_to_postgres(analytics)
    
    # Set up dependencies
    tables >> token >> raw_data >> analytics >> load_data
