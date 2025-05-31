import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, timedelta

# Database connection string
DB_URI = "postgresql://postgres:postgres@localhost:5432/postgres"
engine = create_engine(DB_URI)

st.set_page_config(layout="wide", page_title="Email Analytics Dashboard")
st.title("📧 Email Analytics Dashboard with Response Tracking")

def check_database_connection():
    """Check if database connection is working and show available tables"""
    try:
        # Test connection
        test_query = "SELECT 1"
        pd.read_sql(test_query, engine)
        
        # Get available tables
        tables_query = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name;
        """
        tables_df = pd.read_sql(tables_query, engine)
        return True, tables_df['table_name'].tolist()
    except Exception as e:
        return False, str(e)

def get_table_schema(table_name):
    """Get column information for a specific table"""
    try:
        schema_query = f"""
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns 
        WHERE table_name = '{table_name}' AND table_schema = 'public'
        ORDER BY ordinal_position;
        """
        return pd.read_sql(schema_query, engine)
    except Exception as e:
        return None

@st.cache_data
def load_email_data():
    """Load main email data with error handling"""
    try:
        # First, check what columns exist
        schema = get_table_schema('email_group')
        if schema is None:
            raise Exception("Cannot access email_group table schema")
        
        available_columns = schema['column_name'].tolist()
        st.sidebar.info(f"Available columns in email_group: {', '.join(available_columns)}")
        
        # Build query based on available columns
        query = "SELECT * FROM email_group;"
        df = pd.read_sql(query, engine)
        
        # Handle datetime columns flexibly
        datetime_columns = ['received_datetime', 'sent_datetime', 'date_received', 'timestamp', 'created_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                if col != 'received_datetime':
                    df['received_datetime'] = df[col]  # Standardize column name
                break
        
        # If no datetime column found, create a dummy one
        if 'received_datetime' not in df.columns:
            st.warning("No datetime column found. Using current date as placeholder.")
            df['received_datetime'] = datetime.now()
            
        return df, available_columns
        
    except Exception as e:
        st.error(f"Error loading email data: {e}")
        return pd.DataFrame(), []

@st.cache_data
def load_response_data():
    """Load email relationships data with error handling"""
    try:
        query = "SELECT * FROM email_relationships;"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        st.warning(f"Could not load response relationships: {e}")
        return pd.DataFrame()

@st.cache_data
def load_conversation_analytics():
    """Load conversation analytics data with error handling"""
    try:
        query = "SELECT * FROM conversation_analytics;"
        df = pd.read_sql(query, engine)
        
        # Handle datetime columns
        datetime_columns = ['conversation_start_date', 'last_activity_date']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                
        return df
    except Exception as e:
        st.warning(f"Could not load conversation analytics: {e}")
        return pd.DataFrame()

@st.cache_data
def load_combined_data():
    """Load combined email and response data with error handling"""
    try:
        query = """
        SELECT 
            eg.*,
            er.is_response,
            er.is_from_user,
            er.response_time_hours,
            er.position_in_thread,
            er.thread_length,
            ca.response_rate_percent,
            ca.total_emails as conversation_total_emails
        FROM email_group eg
        LEFT JOIN email_relationships er ON eg.email_id = er.email_id
        LEFT JOIN conversation_analytics ca ON eg.conversation_id = ca.conversation_id
        """
        df = pd.read_sql(query, engine)
        
        # Handle datetime columns flexibly
        datetime_columns = ['received_datetime', 'sent_datetime', 'date_received', 'timestamp', 'created_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col])
                if col != 'received_datetime':
                    df['received_datetime'] = df[col]
                break
        
        if 'received_datetime' not in df.columns:
            df['received_datetime'] = datetime.now()
            
        return df
    except Exception as e:
        st.warning(f"Could not load combined data, falling back to email data only: {e}")
        df, _ = load_email_data()
        return df

# Database Connection Check
st.sidebar.header("🔍 Database Status")
connection_ok, connection_info = check_database_connection()

if not connection_ok:
    st.error(f"❌ Database Connection Failed: {connection_info}")
    st.info("Please check your database connection settings and ensure PostgreSQL is running.")
    st.stop()
else:
    st.sidebar.success("✅ Database Connected")
    with st.sidebar.expander("Available Tables"):
        for table in connection_info:
            st.write(f"- {table}")

# Load Data with Better Error Handling
try:
    # Load all data
    df, available_columns = load_email_data()
    
    if df.empty:
        st.error("No data found in email_group table.")
        st.stop()
    
    response_df = load_response_data()
    conversation_df = load_conversation_analytics()
    combined_df = load_combined_data()
    
    # Show data summary
    st.sidebar.header("📊 Data Summary")
    st.sidebar.metric("Total Emails", len(df))
    st.sidebar.metric("Response Records", len(response_df))
    st.sidebar.metric("Conversations", len(conversation_df))
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Filter Emails")
        
        # Folder filter (if folder column exists)
        if 'folder' in df.columns:
            folders = df['folder'].dropna().unique().tolist()
            if folders:
                selected_folders = st.multiselect("Select Folders", folders, default=folders)
            else:
                selected_folders = []
                st.info("No folder data available")
        else:
            selected_folders = []
            st.info("No folder column found")
        
        # Date filter
        if 'received_datetime' in df.columns and not df['received_datetime'].isna().all():
            date_min, date_max = df['received_datetime'].min(), df['received_datetime'].max()
            selected_dates = st.date_input("Date Range", [date_min.date(), date_max.date()])
        else:
            selected_dates = [datetime.now().date() - timedelta(days=30), datetime.now().date()]
            st.info("Using default date range")
        
        # Response filter
        st.subheader("Response Filters")
        show_responses_only = st.checkbox("Show Only Responses")
        show_user_emails_only = st.checkbox("Show Only User Emails")
        
        # Response time filter
        if not response_df.empty and 'response_time_hours' in response_df.columns:
            max_response_time = response_df['response_time_hours'].max()
            if pd.notna(max_response_time):
                response_time_filter = st.slider(
                    "Max Response Time (hours)", 
                    0.0, 
                    float(max_response_time), 
                    float(max_response_time),
                    step=0.5
                )
    
    # Apply filters
    filtered_df = combined_df.copy()
    
    # Apply folder filter if available
    if selected_folders and 'folder' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['folder'].isin(selected_folders)]
    
    # Apply date filter
    if 'received_datetime' in filtered_df.columns:
        filtered_df = filtered_df[
            (filtered_df['received_datetime'] >= pd.to_datetime(selected_dates[0])) &
            (filtered_df['received_datetime'] <= pd.to_datetime(selected_dates[1]))
        ]
    
    if show_responses_only and 'is_response' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['is_response'] == True]
    
    if show_user_emails_only and 'is_from_user' in filtered_df.columns:
        filtered_df = filtered_df[filtered_df['is_from_user'] == True]
    
    # Main Dashboard
    st.header("📊 Overview Metrics")
    
    # Summary Stats
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Emails", len(filtered_df))
    
    with col2:
        if 'from_address' in filtered_df.columns:
            st.metric("Unique Senders", filtered_df['from_address'].nunique())
        else:
            st.metric("Unique Senders", "N/A")
    
    with col3:
        if not conversation_df.empty and 'response_rate_percent' in conversation_df.columns:
            avg_response_rate = conversation_df['response_rate_percent'].mean()
            st.metric("Avg Response Rate", f"{avg_response_rate:.1f}%")
        else:
            st.metric("Avg Response Rate", "N/A")
    
    with col4:
        user_responses = filtered_df[
            (filtered_df.get('is_response', False) == True) & 
            (filtered_df.get('is_from_user', False) == True)
        ]
        if not user_responses.empty and 'response_time_hours' in user_responses.columns:
            avg_response_time = user_responses['response_time_hours'].mean()
            st.metric("Avg Response Time", f"{avg_response_time:.1f}h")
        else:
            st.metric("Avg Response Time", "N/A")
    
    with col5:
        if not conversation_df.empty:
            st.metric("Active Conversations", len(conversation_df))
        else:
            st.metric("Active Conversations", "N/A")
    
    # Create tabs for different views
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📈 Email Trends", 
        "⚡ Response Analytics", 
        "💬 Conversation Insights", 
        "📋 Detailed Analysis",
        "📊 Raw Data"
    ])
    
    with tab1:
        st.subheader("Email Volume Trends")
        
        if 'received_datetime' in filtered_df.columns and not filtered_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Emails by Date
                by_date = filtered_df.groupby(filtered_df['received_datetime'].dt.date).size().reset_index(name='count')
                by_date.columns = ['date', 'count']
                
                if not by_date.empty:
                    chart = alt.Chart(by_date).mark_line(point=True).encode(
                        x=alt.X('date:T', title='Date'),
                        y=alt.Y('count:Q', title='Number of Emails'),
                        tooltip=['date:T', 'count:Q']
                    ).properties(width=400, height=300, title="Emails Received Over Time")
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No date data available for trending")
            
            with col2:
                # Emails by Folder
                if 'folder' in filtered_df.columns:
                    folder_count = filtered_df.groupby('folder').size().reset_index(name='count')
                    if not folder_count.empty:
                        chart = alt.Chart(folder_count).mark_bar().encode(
                            x=alt.X('count:Q', title='Number of Emails'),
                            y=alt.Y('folder:N', title='Folder', sort='-x'),
                            color=alt.Color('folder:N', legend=None),
                            tooltip=['folder:N', 'count:Q']
                        ).properties(width=400, height=300, title="Emails by Folder")
                        st.altair_chart(chart, use_container_width=True)
                    else:
                        st.info("No folder data available")
                else:
                    st.info("No folder column found")
            
            # Top Senders
            if 'from_address' in filtered_df.columns:
                st.subheader("Top Email Senders")
                top_senders = filtered_df.groupby('from_address').size().reset_index(name='count').sort_values('count', ascending=False).head(10)
                if not top_senders.empty:
                    chart = alt.Chart(top_senders).mark_bar().encode(
                        x=alt.X('count:Q', title='Number of Emails'),
                        y=alt.Y('from_address:N', title='Sender Email', sort='-x'),
                        tooltip=['from_address:N', 'count:Q']
                    ).properties(height=400, title="Most Active Email Senders")
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("No sender data available")
        else:
            st.info("No datetime data available for trends analysis")
    
    with tab2:
        st.subheader("⚡ Response Analytics")
        
        if not response_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Response Time Distribution
                user_responses = filtered_df[
                    (filtered_df.get('is_response', False) == True) & 
                    (filtered_df.get('is_from_user', False) == True) & 
                    (filtered_df['response_time_hours'].notna())
                ] if 'response_time_hours' in filtered_df.columns else pd.DataFrame()
                
                if not user_responses.empty:
                    fig = px.histogram(
                        user_responses, 
                        x='response_time_hours', 
                        nbins=20,
                        title="Response Time Distribution",
                        labels={'response_time_hours': 'Response Time (Hours)', 'count': 'Frequency'}
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No response time data available")
            
            with col2:
                # Response Rate by Conversation
                if not conversation_df.empty and 'response_rate_percent' in conversation_df.columns:
                    fig = px.histogram(
                        conversation_df, 
                        x='response_rate_percent', 
                        nbins=10,
                        title="Response Rate Distribution",
                        labels={'response_rate_percent': 'Response Rate (%)', 'count': 'Number of Conversations'}
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No conversation analytics data available")
        else:
            st.info("No response analytics data available. Make sure the DAG has run and populated the email_relationships table.")
    
    with tab3:
        st.subheader("💬 Conversation Insights")
        
        if not conversation_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Conversation Length Distribution
                if 'total_emails' in conversation_df.columns:
                    fig = px.histogram(
                        conversation_df, 
                        x='total_emails', 
                        nbins=15,
                        title="Conversation Length Distribution",
                        labels={'total_emails': 'Emails per Conversation', 'count': 'Number of Conversations'}
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("No conversation length data available")
            
            with col2:
                # User Participation Rate
                required_cols = ['total_emails', 'response_rate_percent', 'user_emails_count']
                if all(col in conversation_df.columns for col in required_cols):
                    fig = px.scatter(
                        conversation_df, 
                        x='total_emails', 
                        y='response_rate_percent',
                        size='user_emails_count',
                        hover_data=['external_emails_count'] if 'external_emails_count' in conversation_df.columns else None,
                        title="Response Rate vs Conversation Length",
                        labels={
                            'total_emails': 'Total Emails in Conversation',
                            'response_rate_percent': 'Response Rate (%)',
                            'user_emails_count': 'Your Emails'
                        }
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info("Insufficient data for participation analysis")
        else:
            st.info("No conversation analytics data available.")
    
    with tab4:
        st.subheader("📋 Detailed Analysis")
        st.info("Detailed analysis will be shown here when more complete data is available.")
        
        # Show available columns for debugging
        with st.expander("Available Data Columns"):
            st.write("**Email Data Columns:**", list(df.columns))
            if not response_df.empty:
                st.write("**Response Data Columns:**", list(response_df.columns))
            if not conversation_df.empty:
                st.write("**Conversation Data Columns:**", list(conversation_df.columns))
    
    with tab5:
        st.subheader("📊 Raw Data Tables")
        
        # Choose which table to view
        table_choice = st.selectbox(
            "Select Table to View",
            ["Email Data", "Response Relationships", "Conversation Analytics", "Combined Data"]
        )
        
        if table_choice == "Email Data":
            st.dataframe(df, use_container_width=True)
        elif table_choice == "Response Relationships":
            if not response_df.empty:
                st.dataframe(response_df, use_container_width=True)
            else:
                st.info("No response relationships data available")
        elif table_choice == "Conversation Analytics":
            if not conversation_df.empty:
                st.dataframe(conversation_df, use_container_width=True)
            else:
                st.info("No conversation analytics data available")
        elif table_choice == "Combined Data":
            st.dataframe(filtered_df, use_container_width=True)

except Exception as e:
    st.error(f"An error occurred while loading the dashboard: {e}")
    
    # Enhanced debug information
    with st.expander("🔍 Debug Information", expanded=True):
        st.write("**Expected tables and columns:**")
        st.write("- email_group: email_id, from_address, subject, received_datetime/date_received/timestamp")
        st.write("- email_relationships: email_id, is_response, is_from_user, response_time_hours")  
        st.write("- conversation_analytics: conversation_id, total_emails, response_rate_percent")
        
        st.write(f"**Connection string:** {DB_URI}")
        st.write(f"**Error details:** {str(e)}")
        
        # Show actual database schema
        try:
            connection_ok, available_tables = check_database_connection()
            if connection_ok:
                st.write(f"**Available tables:** {', '.join(available_tables)}")
                
                for table in ['email_group', 'email_relationships', 'conversation_analytics']:
                    if table in available_tables:
                        schema = get_table_schema(table)
                        if schema is not None:
                            st.write(f"**{table} schema:**")
                            st.dataframe(schema, use_container_width=True)
            else:
                st.write("Could not connect to database to show schema")
        except Exception as debug_e:
            st.write(f"Could not retrieve debug information: {debug_e}")
