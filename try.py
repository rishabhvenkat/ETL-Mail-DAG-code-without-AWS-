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
import traceback

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_CONN_ID = 'postgres_default'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id='ms_graph_email_etl_postgres_with_analytics_debug',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    tags=['email', 'graph', 'etl', 'postgres', 'analytics', 'debug']
) as dag:

    @task()
    def test_connections():
        """Test all connections and prerequisites"""
        try:
            logger.info("=== STARTING CONNECTION TESTS ===")
            
            # Test PostgreSQL connection
            logger.info("Testing PostgreSQL connection...")
            try:
                pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
                conn = pg_hook.get_conn()
                cursor = conn.cursor()
                cursor.execute("SELECT version();")
                result = cursor.fetchone()
                logger.info(f"✅ PostgreSQL connection successful: {result[0]}")
                cursor.close()
                conn.close()
            except Exception as e:
                logger.error(f"❌ PostgreSQL connection failed: {str(e)}")
                raise
            
            # Test environment variables
            logger.info("Testing MS Graph environment variables...")
            required_env_vars = ["MS_GRAPH_TENANT_ID", "MS_GRAPH_CLIENT_ID", "MS_GRAPH_CLIENT_SECRET"]
            for var in required_env_vars:
                value = os.environ.get(var)
                if value:
                    logger.info(f"✅ {var}: {'*' * min(len(value), 10)}...")
                else:
                    logger.error(f"❌ {var}: NOT SET")
                    raise ValueError(f"Missing environment variable: {var}")
            
            # Test Airflow variables
            logger.info("Testing Airflow variables...")
            try:
                user_email = Variable.get("MS_GRAPH_USER_EMAIL")
                logger.info(f"✅ MS_GRAPH_USER_EMAIL: {user_email}")
            except Exception as e:
                logger.error(f"❌ MS_GRAPH_USER_EMAIL variable not set: {str(e)}")
                raise
                
            try:
                folder_names = Variable.get("MS_GRAPH_FOLDER_NAMES", default='["Inbox", "Sent Items"]')
                folders = json.loads(folder_names)
                logger.info(f"✅ MS_GRAPH_FOLDER_NAMES: {folders}")
            except Exception as e:
                logger.error(f"❌ MS_GRAPH_FOLDER_NAMES variable issue: {str(e)}")
                raise
            
            logger.info("=== ALL CONNECTION TESTS PASSED ===")
            return {"status": "success", "message": "All connections tested successfully"}
            
        except Exception as e:
            logger.error(f"=== CONNECTION TEST FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @task()
    def create_tables():
        """Create required tables and add missing columns if they don't exist"""
        try:
            logger.info("=== STARTING TABLE CREATION ===")
            
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            # Test connection
            cursor.execute("SELECT 1;")
            result = cursor.fetchone()
            logger.info(f"✅ Connection test successful: {result}")
            
            # Create email_group table
            logger.info("Creating email_group table...")
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
            logger.info("✅ email_group table created/verified")
            
            # Create email_relationships table
            logger.info("Creating email_relationships table...")
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
            logger.info("✅ email_relationships table created/verified")
            
            # Create conversation_analytics table
            logger.info("Creating conversation_analytics table...")
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
            logger.info("✅ conversation_analytics table created/verified")
            
            conn.commit()
            logger.info("=== TABLE CREATION COMPLETED SUCCESSFULLY ===")
            
            return {"status": "success", "message": "Tables created successfully"}
            
        except Exception as e:
            logger.error(f"=== TABLE CREATION FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise
        finally:
            try:
                if 'cursor' in locals():
                    cursor.close()
                if 'conn' in locals():
                    conn.close()
                logger.info("Database connections closed")
            except:
                pass

    @task()
    def get_graph_token():
        try:
            logger.info("=== STARTING TOKEN ACQUISITION ===")
            
            tenant_id = os.environ.get("MS_GRAPH_TENANT_ID")
            client_id = os.environ.get("MS_GRAPH_CLIENT_ID")
            client_secret = os.environ.get("MS_GRAPH_CLIENT_SECRET")

            if not all([tenant_id, client_id, client_secret]):
                raise ValueError("Missing required environment variables")

            url = f"https://login.microsoftonline.com/{tenant_id}/oauth2/v2.0/token"
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
            data = {
                "client_id": client_id,
                "client_secret": client_secret,
                "grant_type": "client_credentials",
                "scope": "https://graph.microsoft.com/.default"
            }

            logger.info(f"Making token request to: {url}")
            response = requests.post(url, headers=headers, data=data)
            
            if response.status_code == 200:
                logger.info("✅ Token acquired successfully")
                return response.json()['access_token']
            else:
                logger.error(f"❌ Token acquisition failed: {response.status_code} - {response.text}")
                raise Exception(f"Failed to get token: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"=== TOKEN ACQUISITION FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @task()
    def extract_limited_emails(access_token: str):
        """Extract a limited number of emails for testing"""
        try:
            logger.info("=== STARTING LIMITED EMAIL EXTRACTION ===")
            
            graph_base = "https://graph.microsoft.com/v1.0"
            user_email = Variable.get("MS_GRAPH_USER_EMAIL")
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "Accept": "application/json"
            }

            # Just get Inbox for testing
            url = f"{graph_base}/users/{user_email}/mailFolders/Inbox/messages"
            url += "?$select=id,conversationId,subject,from,toRecipients,ccRecipients,receivedDateTime,sentDateTime,isRead,hasAttachments,importance,internetMessageId"
            url += "&$top=10"  # Limit to 10 emails for testing
            
            logger.info(f"Making request to: {url}")
            response = requests.get(url, headers=headers)
            
            if response.status_code == 200:
                data = response.json()
                emails = data.get("value", [])
                logger.info(f"✅ Successfully extracted {len(emails)} emails for testing")
                
                # Log sample email structure
                if emails:
                    sample_email = emails[0]
                    logger.info(f"Sample email structure: {list(sample_email.keys())}")
                
                return {
                    "emails_by_folder": {"Inbox": emails},
                    "user_email": user_email,
                    "total_count": len(emails)
                }
            else:
                logger.error(f"❌ Email extraction failed: {response.status_code} - {response.text}")
                raise Exception(f"Failed to extract emails: {response.status_code} - {response.text}")
                
        except Exception as e:
            logger.error(f"=== EMAIL EXTRACTION FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @task()
    def analyze_email_responses(data):
        try:
            logger.info("=== STARTING EMAIL ANALYSIS ===")
            logger.info(f"Input data keys: {list(data.keys())}")
            
            all_emails = data.get("emails_by_folder", {})
            user_email = data.get("user_email", "")
            total_count = data.get("total_count", 0)

            logger.info(f"Analyzing {total_count} emails")

            all_email_list = []
            for folder, emails in all_emails.items():
                for email in emails:
                    email['folder'] = folder
                    all_email_list.append(email)

            logger.info(f"Processing {len(all_email_list)} emails")

            # Simple analysis for testing
            email_relationships = []
            conversation_analytics = []
            
            conversations = {}
            for email in all_email_list:
                conv_id = email.get('conversationId')
                if conv_id:
                    conversations.setdefault(conv_id, []).append(email)

            logger.info(f"Found {len(conversations)} conversations")

            for conv_id, emails in conversations.items():
                for i, email in enumerate(emails):
                    from_email = email.get('from', {}).get('emailAddress', {}).get('address', '')
                    is_from_user = from_email.lower() == user_email.lower()

                    email_relationships.append({
                        'email_id': email.get('id'),
                        'conversation_id': conv_id,
                        'is_response': i > 0,
                        'is_from_user': is_from_user,
                        'original_email_id': None,
                        'response_time_hours': None,
                        'position_in_thread': i + 1,
                        'thread_length': len(emails)
                    })

                conversation_analytics.append({
                    'conversation_id': conv_id,
                    'total_emails': len(emails),
                    'user_emails_count': len([e for e in emails if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() == user_email.lower()]),
                    'external_emails_count': len([e for e in emails if e.get('from', {}).get('emailAddress', {}).get('address', '').lower() != user_email.lower()]),
                    'user_response_count': 0,
                    'response_rate_percent': 0,
                    'conversation_start_date': emails[0].get('receivedDateTime', '')[:10] if emails else '',
                    'last_activity_date': emails[-1].get('receivedDateTime', '')[:10] if emails else ''
                })

            logger.info(f"✅ Analysis completed: {len(email_relationships)} relationships, {len(conversation_analytics)} analytics")

            return {
                "emails_by_folder": all_emails,
                "user_email": user_email,
                "email_relationships": email_relationships,
                "conversation_analytics": conversation_analytics,
                "total_count": total_count
            }
            
        except Exception as e:
            logger.error(f"=== EMAIL ANALYSIS FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            raise

    @task()
    def load_to_postgres_debug(data):
        """Debug version of load_to_postgres with extensive logging"""
        conn = None
        cursor = None
        try:
            logger.info("=== STARTING POSTGRES LOAD (DEBUG MODE) ===")
            logger.info(f"Received data keys: {list(data.keys())}")
            
            # Extract data
            emails = data.get('emails_by_folder', {})
            relationships = data.get('email_relationships', [])
            analytics = data.get('conversation_analytics', [])
            total_count = data.get('total_count', 0)

            logger.info(f"Data summary:")
            logger.info(f"  - Total emails: {total_count}")
            logger.info(f"  - Folders: {list(emails.keys())}")
            logger.info(f"  - Relationships: {len(relationships)}")
            logger.info(f"  - Analytics: {len(analytics)}")

            # Test PostgreSQL connection
            logger.info("Testing PostgreSQL connection...")
            pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
            conn = pg_hook.get_conn()
            cursor = conn.cursor()
            
            cursor.execute("SELECT 1;")
            logger.info("✅ PostgreSQL connection established")

            # Check if tables exist
            cursor.execute("""
                SELECT table_name FROM information_schema.tables 
                WHERE table_schema = 'public' AND table_name IN ('email_group', 'email_relationships', 'conversation_analytics');
            """)
            existing_tables = [row[0] for row in cursor.fetchall()]
            logger.info(f"Existing tables: {existing_tables}")

            # Load emails (limited for debugging)
            email_count = 0
            for folder, email_list in emails.items():
                logger.info(f"Processing {len(email_list)} emails from folder '{folder}'")
                
                for i, email in enumerate(email_list[:5]):  # Limit to 5 emails for debugging
                    try:
                        email_id = email.get('id')
                        if not email_id:
                            logger.warning(f"Skipping email with no ID")
                            continue

                        logger.info(f"Processing email {i+1}: {email_id}")
                        
                        # Extract data safely
                        conversation_id = email.get('conversationId')
                        subject = email.get('subject', '')[:500]  # Limit subject length
                        
                        from_address = ''
                        if email.get('from') and email.get('from').get('emailAddress'):
                            from_address = email.get('from').get('emailAddress').get('address', '')
                        
                        to_recipients = []
                        if email.get('toRecipients'):
                            to_recipients = [r.get('emailAddress', {}).get('address', '') for r in email.get('toRecipients', []) if r.get('emailAddress')]
                        
                        cc_recipients = []
                        if email.get('ccRecipients'):
                            cc_recipients = [r.get('emailAddress', {}).get('address', '') for r in email.get('ccRecipients', []) if r.get('emailAddress')]

                        logger.info(f"  Email data: subject='{subject[:50]}...', from='{from_address}', to_count={len(to_recipients)}")

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
                            email_id,
                            conversation_id,
                            subject,
                            from_address,
                            json.dumps(to_recipients),
                            json.dumps(cc_recipients),
                            email.get('receivedDateTime'),
                            email.get('sentDateTime'),
                            email.get('isRead', False),
                            email.get('hasAttachments', False),
                            email.get('importance', 'normal'),
                            folder,
                            email.get('internetMessageId')
                        ))
                        
                        email_count += 1
                        logger.info(f"  ✅ Email {i+1} inserted successfully")
                        
                    except Exception as e:
                        logger.error(f"  ❌ Error inserting email {i+1}: {str(e)}")
                        logger.error(f"  Email data: {email}")
                        continue

            conn.commit()
            logger.info(f"✅ Successfully loaded {email_count} emails")

            # Load a few relationships for testing
            rel_count = 0
            for rel in relationships[:5]:  # Limit for debugging
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
                        rel['email_id'], rel['conversation_id'], rel['is_response'], 
                        rel['is_from_user'], rel['original_email_id'], rel['response_time_hours'], 
                        rel['position_in_thread'], rel['thread_length']
                    ))
                    rel_count += 1
                except Exception as e:
                    logger.error(f"Error inserting relationship: {str(e)}")
                    continue

            conn.commit()
            logger.info(f"✅ Successfully loaded {rel_count} relationships")

            # Load analytics
            analytics_count = 0
            for metric in analytics[:5]:  # Limit for debugging
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
                        metric['response_rate_percent'], metric['conversation_start_date'], 
                        metric['last_activity_date']
                    ))
                    analytics_count += 1
                except Exception as e:
                    logger.error(f"Error inserting analytics: {str(e)}")
                    continue

            conn.commit()
            logger.info(f"✅ Successfully loaded {analytics_count} analytics records")

            logger.info("=== POSTGRES LOAD COMPLETED SUCCESSFULLY ===")
            return {
                "status": "success",
                "emails_loaded": email_count,
                "relationships_loaded": rel_count,
                "analytics_loaded": analytics_count
            }

        except Exception as e:
            logger.error(f"=== POSTGRES LOAD FAILED ===")
            logger.error(f"Error: {str(e)}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            if conn:
                conn.rollback()
            raise
        finally:
            try:
                if cursor:
                    cursor.close()
                if conn:
                    conn.close()
                logger.info("Database connections closed")
            except Exception as e:
                logger.error(f"Error closing connections: {str(e)}")

    # Define task dependencies with proper testing flow
    test_conn = test_connections()
    tables = create_tables()
    token = get_graph_token()
    raw_data = extract_limited_emails(token)
    analytics = analyze_email_responses(raw_data)
    load_data = load_to_postgres_debug(analytics)
    
    # Set up dependencies
    test_conn >> tables >> token >> raw_data >> analytics >> load_data
