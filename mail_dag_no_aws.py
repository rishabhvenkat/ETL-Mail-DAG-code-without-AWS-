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
            if folder_name.strip().lower() == "inbox":
                inbox_url = f"{graph_base}/users/{user_email}/mailFolders/Inbox"
                response = requests.get(inbox_url, headers=headers)
                if response.status_code == 200:
                    folder = response.json()
                    return folder["id"]
                else:
                    raise Exception(f"Error fetching Inbox folder: {response.status_code}, {response.text}")
            elif folder_name.strip().lower() == "sent items":
                sent_url = f"{graph_base}/users/{user_email}/mailFolders/SentItems"
                response = requests.get(sent_url, headers=headers)
                if response.status_code == 200:
                    folder = response.json()
                    return folder["id"]
                else:
                    raise Exception(f"Error fetching Sent Items folder: {response.status_code}, {response.text}")
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
            # Enhanced query to get more email details for analytics
            url = f"{graph_base}/users/{user_email}/mailFolders/{folder_id}/messages?$select=id,conversationId,subject,from,toRecipients,ccRecipients,receivedDateTime,sentDateTime,isRead,hasAttachments,importance,flag,internetMessageId&$orderby=receivedDateTime desc"
            while url:
                response = requests.get(url, headers=headers)
                if response.status_code == 200:
                    data = response.json()['access_token']
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
        
        logging.info(f"User: {user_email}")
        for folder, emails in all_emails.items():
            logging.info(f"Folder '{folder}': {len(emails)} emails")

        return {"emails_by_folder": all_emails, "user_email": user_email}

    @task()
    def analyze_email_responses(data):
        """Analyze email responses and create response analytics"""
        if data is None:
            raise ValueError("No data passed to analyze_email_responses")

        all_emails = data.get("emails_by_folder", {})
        user_email = data.get("user_email", "")
        
        # Combine all emails for analysis
        all_email_list = []
        for folder_name, emails in all_emails.items():
            for email in emails:
                email['folder'] = folder_name
                all_email_list.append(email)
        
        # Group emails by conversation thread
        conversations = {}
        for email in all_email_list:
            conv_id = email.get('conversationId')
            if conv_id:
                if conv_id not in conversations:
                    conversations[conv_id] = []
                conversations[conv_id].append(email)
        
        # Analyze response patterns
        response_analytics = []
        email_relationships = []
        
        for conv_id, emails in conversations.items():
            # Sort emails by received/sent date
            emails_sorted = sorted(emails, key=lambda x: x.get('receivedDateTime', x.get('sentDateTime', '')))
            
            for i, email in enumerate(emails_sorted):
                is_response = False
                is_from_user = False
                response_time_hours = None
                original_email_id = None
                
                # Check if email is from the user
                from_email = email.get('from', {}).get('emailAddress', {}).get('address', '')
                is_from_user = from_email.lower() == user_email.lower()
                
                # Check if this is a response (not the first email in conversation)
                if i > 0:
                    is_response = True
                    original_email = emails_sorted[i-1]
                    original_email_id = original_email.get('id')
                    
                    # Calculate response time
                    if email.get('receivedDateTime') and original_email.get('receivedDateTime'):
                        try:
                            current_time = datetime.fromisoformat(email.get('receivedDateTime').replace('Z', '+00:00'))
                            original_time = datetime.fromisoformat(original_email.get('receivedDateTime').replace('Z', '+00:00'))
                            response_time_hours = (current_time - original_time).total_seconds() / 3600
                        except:
                            response_time_hours = None
                
                # Create email relationship record
                email_relationships.append({
                    'email_id': email.get('id'),
                    'conversation_id': conv_id,
                    'is_response': is_response,
                    'is_from_user': is_from_user,
                    'original_email_id': original_email_id,
                    'response_time_hours': response_time_hours,
                    'position_in_thread': i + 1,
                    'thread_length': len(emails_sorted)
                })
        
        # Create conversation-level analytics
        conversation_analytics = []
        for conv_id, emails in conversations.items():
            user_emails = [e for e in emails if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() == user_email.lower()]
            external_emails = [e for e in emails if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() != user_email.lower()]
            
            # Calculate response rate (user responses to external emails)
            user_response_count = 0
            external_requiring_response = 0
            
            emails_sorted = sorted(emails, key=lambda x: x.get('receivedDateTime', x.get('sentDateTime', '')))
            
            for i, email in enumerate(emails_sorted[:-1]):  # Exclude last email as it can't have a response yet
                from_email = email.get('from', {}).get('emailAddress', {}).get('address', '')
                if from_email.lower() != user_email.lower():  # External email
                    external_requiring_response += 1
                    # Check if next email is from user (response)
                    if i + 1 < len(emails_sorted):
                        next_email = emails_sorted[i + 1]
                        next_from = next_email.get('from', {}).get('emailAddress', {}).get('address', '')
                        if next_from.lower() == user_email.lower():
                            user_response_count += 1
            
            response_rate = (user_response_count / external_requiring_response * 100) if external_requiring_response > 0 else 0
            
            conversation_analytics.append({
                'conversation_id': conv_id,
                'total_emails': len(emails),
                'user_emails_count': len(user_emails),
                'external_emails_count': len(external_emails),
                'user_response_count': user_response_count,
                'response_rate_percent': round(response_rate, 2),
                'conversation_start_date': min([e.get('receivedDateTime', e.get('sentDateTime', '')) for e in emails])[:10],
                'last_activity_date': max([e.get('receivedDateTime', e.get('sentDateTime', '')) for e in emails])[:10]
            })
        
        logging.info(f"Analyzed {len(conversations)} conversations")
        logging.info(f"Created {len(email_relationships)} email relationship records")
        logging.info(f"Created {len(conversation_analytics)} conversation analytics records")
        
        return {
            "emails_by_folder": all_emails,
            "user_email": user_email,
            "email_relationships": email_relationships,
            "conversation_analytics": conversation_analytics
        }

    @task()
    def load_to_postgres(data):
        if data is None:
            raise ValueError("No data passed to load_to_postgres")

        logging.info("Loading data to PostgreSQL...")

        all_emails = data.get("emails_by_folder", {})
        email_relationships = data.get("email_relationships", [])
        conversation_analytics = data.get("conversation_analytics", [])

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create main emails table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_group (
                email_id TEXT PRIMARY KEY,
                thread_id TEXT,
                from_name TEXT,
                from_email TEXT,
                subject TEXT,
                received_date DATE,
                received_time TIME,
                sent_date DATE,
                sent_time TIME,
                folder TEXT,
                is_read BOOLEAN,
                has_attachments BOOLEAN,
                importance TEXT,
                internet_message_id TEXT,
                to_recipients TEXT,
                cc_recipients TEXT
            );
        """)

        # Create email relationships table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS email_relationships (
                email_id TEXT PRIMARY KEY,
                conversation_id TEXT,
                is_response BOOLEAN,
                is_from_user BOOLEAN,
                original_email_id TEXT,
                response_time_hours NUMERIC,
                position_in_thread INTEGER,
                thread_length INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (email_id) REFERENCES email_group(email_id)
            );
        """)

        # Create conversation analytics table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS conversation_analytics (
                conversation_id TEXT PRIMARY KEY,
                total_emails INTEGER,
                user_emails_count INTEGER,
                external_emails_count INTEGER,
                user_response_count INTEGER,
                response_rate_percent NUMERIC,
                conversation_start_date DATE,
                last_activity_date DATE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # Insert emails
        for folder_name, emails in all_emails.items():
            for msg in emails:
                try:
                    # Process recipients
                    to_recipients = []
                    if msg.get('toRecipients'):
                        to_recipients = [r.get('emailAddress', {}).get('address', '') for r in msg.get('toRecipients', [])]
                    
                    cc_recipients = []
                    if msg.get('ccRecipients'):
                        cc_recipients = [r.get('emailAddress', {}).get('address', '') for r in msg.get('ccRecipients', [])]

                    cursor.execute("""
                        INSERT INTO email_group (
                            email_id, thread_id, from_name, from_email,
                            subject, received_date, received_time, sent_date, sent_time,
                            folder, is_read, has_attachments, importance,
                            internet_message_id, to_recipients, cc_recipients
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (email_id) DO UPDATE SET
                            is_read = EXCLUDED.is_read,
                            folder = EXCLUDED.folder;
                    """, (
                        msg['id'],
                        msg.get('conversationId'),
                        msg.get('from', {}).get('emailAddress', {}).get('name'),
                        msg.get('from', {}).get('emailAddress', {}).get('address'),
                        msg.get('subject', ''),
                        msg.get('receivedDateTime', '')[:10] if msg.get('receivedDateTime') else None,
                        msg.get('receivedDateTime', '')[11:19] if msg.get('receivedDateTime') else None,
                        msg.get('sentDateTime', '')[:10] if msg.get('sentDateTime') else None,
                        msg.get('sentDateTime', '')[11:19] if msg.get('sentDateTime') else None,
                        folder_name,
                        msg.get('isRead', False),
                        msg.get('hasAttachments', False),
                        msg.get('importance', 'normal'),
                        msg.get('internetMessageId'),
                        ','.join(to_recipients),
                        ','.join(cc_recipients)
                    ))
                except Exception as e:
                    logging.error(f"Failed to insert email id {msg.get('id')}: {e}")

        # Insert email relationships
        for rel in email_relationships:
            try:
                cursor.execute("""
                    INSERT INTO email_relationships (
                        email_id, conversation_id, is_response, is_from_user,
                        original_email_id, response_time_hours, position_in_thread, thread_length
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (email_id) DO UPDATE SET
                        response_time_hours = EXCLUDED.response_time_hours,
                        position_in_thread = EXCLUDED.position_in_thread,
                        thread_length = EXCLUDED.thread_length;
                """, (
                    rel['email_id'],
                    rel['conversation_id'],
                    rel['is_response'],
                    rel['is_from_user'],
                    rel['original_email_id'],
                    rel['response_time_hours'],
                    rel['position_in_thread'],
                    rel['thread_length']
                ))
            except Exception as e:
                logging.error(f"Failed to insert email relationship for {rel.get('email_id')}: {e}")

        # Insert conversation analytics
        for conv in conversation_analytics:
            try:
                cursor.execute("""
                    INSERT INTO conversation_analytics (
                        conversation_id, total_emails, user_emails_count, external_emails_count,
                        user_response_count, response_rate_percent, conversation_start_date, last_activity_date
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (conversation_id) DO UPDATE SET
                        total_emails = EXCLUDED.total_emails,
                        user_emails_count = EXCLUDED.user_emails_count,
                        external_emails_count = EXCLUDED.external_emails_count,
                        user_response_count = EXCLUDED.user_response_count,
                        response_rate_percent = EXCLUDED.response_rate_percent,
                        last_activity_date = EXCLUDED.last_activity_date;
                """, (
                    conv['conversation_id'],
                    conv['total_emails'],
                    conv['user_emails_count'],
                    conv['external_emails_count'],
                    conv['user_response_count'],
                    conv['response_rate_percent'],
                    conv['conversation_start_date'],
                    conv['last_activity_date']
                ))
            except Exception as e:
                logging.error(f"Failed to insert conversation analytics for {conv.get('conversation_id')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        
        logging.info(f"Successfully loaded {len(all_emails)} email folders to PostgreSQL")
        logging.info(f"Successfully loaded {len(email_relationships)} email relationships")
        logging.info(f"Successfully loaded {len(conversation_analytics)} conversation analytics")

    @task()
    def generate_response_analytics_summary():
        """Generate summary analytics about email response patterns"""
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Overall response rate
        overall_stats = pg_hook.get_first("""
            SELECT 
                AVG(response_rate_percent) as avg_response_rate,
                COUNT(*) as total_conversations,
                AVG(total_emails) as avg_emails_per_conversation,
                AVG(user_emails_count::FLOAT / total_emails * 100) as avg_user_participation_rate
            FROM conversation_analytics
            WHERE external_emails_count > 0
        """)
        
        # Response time analytics
        response_time_stats = pg_hook.get_first("""
            SELECT 
                AVG(response_time_hours) as avg_response_time_hours,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time_hours) as median_response_time_hours,
                MIN(response_time_hours) as min_response_time_hours,
                MAX(response_time_hours) as max_response_time_hours
            FROM email_relationships 
            WHERE is_response = true AND is_from_user = true AND response_time_hours IS NOT NULL
        """)
        
        # Daily response patterns
        daily_patterns = pg_hook.get_records("""
            SELECT 
                DATE(eg.received_date) as response_date,
                COUNT(*) as responses_sent,
                AVG(er.response_time_hours) as avg_response_time
            FROM email_group eg
            JOIN email_relationships er ON eg.email_id = er.email_id
            WHERE er.is_response = true AND er.is_from_user = true
            GROUP BY DATE(eg.received_date)
            ORDER BY response_date DESC
            LIMIT 30
        """)
        
        logging.info("=== EMAIL RESPONSE ANALYTICS SUMMARY ===")
        if overall_stats:
            logging.info(f"Average Response Rate: {overall_stats[0]:.1f}%")
            logging.info(f"Total Conversations: {overall_stats[1]}")
            logging.info(f"Average Emails per Conversation: {overall_stats[2]:.1f}")
            logging.info(f"Average User Participation Rate: {overall_stats[3]:.1f}%")
        
        if response_time_stats:
            logging.info(f"Average Response Time: {response_time_stats[0]:.1f} hours")
            logging.info(f"Median Response Time: {response_time_stats[1]:.1f} hours")
            logging.info(f"Fastest Response: {response_time_stats[2]:.1f} hours")
            logging.info(f"Slowest Response: {response_time_stats[3]:.1f} hours")
        
        logging.info("Recent Daily Response Patterns:")
        for pattern in daily_patterns[:10]:  # Show last 10 days
            logging.info(f"  {pattern[0]}: {pattern[1]} responses, avg time: {pattern[2]:.1f}h")
        
        return {
            "overall_stats": overall_stats,
            "response_time_stats": response_time_stats,
            "daily_patterns": daily_patterns
        }

    # DAG task flow
    token = get_graph_token()
    email_data = extract_emails_from_folders(token)
    analyzed_data = analyze_email_responses(email_data)
    load_to_postgres(analyzed_data)
    analytics_summary = generate_response_analytics_summary()
