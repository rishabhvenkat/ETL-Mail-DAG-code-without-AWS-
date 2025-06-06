import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import psycopg2
from datetime import datetime, timedelta, date
import json
from typing import Dict, List, Tuple
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Email Analytics Dashboard",
    page_icon="📧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection configuration
@st.cache_resource
def init_connection():
    """Initialize database connection"""
    return psycopg2.connect(
        host="localhost",
        database="postgres",  
        user="postgres",
        password="postgres",
        port="5432"
    )

@st.cache_data(ttl=300)  # Cache for 5 minutes
def load_email_data():
    """Load email data from PostgreSQL"""
    conn = init_connection()
    
    # Load main email data
    email_query = """
    SELECT 
        email_id, conversation_id, subject, from_address, 
        to_recipients, cc_recipients, received_datetime, 
        sent_datetime, is_read, has_attachments, importance, 
        folder, internet_message_id, created_at
    FROM email_group 
    ORDER BY received_datetime DESC
    """
    
    # Load relationship data
    relationship_query = """
    SELECT * FROM email_relationships
    ORDER BY conversation_id, position_in_thread
    """
    
    # Load analytics data
    analytics_query = """
    SELECT * FROM conversation_analytics
    ORDER BY last_activity_date DESC
    """
    
    emails_df = pd.read_sql(email_query, conn)
    relationships_df = pd.read_sql(relationship_query, conn)
    analytics_df = pd.read_sql(analytics_query, conn)
    
    conn.close()
    
    return emails_df, relationships_df, analytics_df

def process_email_data(emails_df, relationships_df, analytics_df):
    """Process and clean email data"""
    if emails_df.empty:
        return pd.DataFrame(), analytics_df
    
    # Convert datetime columns
    emails_df['received_datetime'] = pd.to_datetime(emails_df['received_datetime'], errors='coerce')
    emails_df['sent_datetime'] = pd.to_datetime(emails_df['sent_datetime'], errors='coerce')

    # Log how many rows have missing datetime before filtering
    missing_datetime = emails_df['received_datetime'].isna().sum()
    if missing_datetime > 0:
        st.warning(f"Found {missing_datetime} emails with missing received_datetime - these will be filtered out")

    # Drop rows where 'received_datetime' is missing (or handle differently if preferred)
    emails_df = emails_df[emails_df['received_datetime'].notna()].copy()

    if emails_df.empty:
        st.error("All emails were filtered out due to missing received_datetime")
        return pd.DataFrame(), analytics_df

    # Convert to proper date objects for Streamlit compatibility
    emails_df['date'] = emails_df['received_datetime'].dt.date
    emails_df['hour'] = emails_df['received_datetime'].dt.hour
    emails_df['weekday'] = emails_df['received_datetime'].dt.day_name()
    
    # Process recipients data
    emails_df['to_count'] = emails_df['to_recipients'].apply(
        lambda x: len(json.loads(x)) if pd.notna(x) and x != '[]' else 0
    )
    emails_df['cc_count'] = emails_df['cc_recipients'].apply(
        lambda x: len(json.loads(x)) if pd.notna(x) and x != '[]' else 0
    )
    
    # Merge with relationships data
    merged_df = emails_df.merge(relationships_df, on='email_id', how='left', suffixes=('', '_rel'))
    
    return merged_df, analytics_df

def create_overview_metrics(emails_df, analytics_df):
    """Create overview metrics"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_emails = len(emails_df)
        st.metric("Total Emails", f"{total_emails:,}")
    
    with col2:
        total_conversations = analytics_df['conversation_id'].nunique() if not analytics_df.empty else 0
        st.metric("Total Conversations", f"{total_conversations:,}")
    
    with col3:
        avg_response_rate = analytics_df['response_rate_percent'].mean() if not analytics_df.empty else 0
        st.metric("Avg Response Rate", f"{avg_response_rate:.1f}%")
    
    with col4:
        unread_emails = emails_df[emails_df['is_read'] == False].shape[0]
        st.metric("Unread Emails", f"{unread_emails:,}")

def create_incoming_analytics(emails_df, merged_df):
    """Create incoming email analytics"""
    st.header("📥 Incoming Email Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Email volume by date
        daily_emails = emails_df.groupby('date').size().reset_index(name='count')
        fig_daily = px.line(
            daily_emails, x='date', y='count',
            title='Daily Email Volume',
            labels={'count': 'Number of Emails', 'date': 'Date'}
        )
        fig_daily.update_traces(line_color='#1f77b4', line_width=2)
        st.plotly_chart(fig_daily, use_container_width=True)
    
    with col2:
        # Email by folder
        folder_counts = emails_df['folder'].value_counts()
        fig_folders = px.pie(
            values=folder_counts.values,
            names=folder_counts.index,
            title='Emails by Folder'
        )
        st.plotly_chart(fig_folders, use_container_width=True)
    
    col3, col4 = st.columns(2)
    
    with col3:
        # Hourly distribution
        hourly_dist = emails_df.groupby('hour').size().reset_index(name='count')
        fig_hourly = px.bar(
            hourly_dist, x='hour', y='count',
            title='Email Distribution by Hour of Day',
            labels={'count': 'Number of Emails', 'hour': 'Hour (24h format)'}
        )
        st.plotly_chart(fig_hourly, use_container_width=True)
    
    with col4:
        # Weekday distribution
        weekday_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        weekday_dist = emails_df.groupby('weekday').size().reindex(weekday_order).reset_index(name='count')
        fig_weekday = px.bar(
            weekday_dist, x='weekday', y='count',
            title='Email Distribution by Day of Week',
            labels={'count': 'Number of Emails', 'weekday': 'Day of Week'}
        )
        st.plotly_chart(fig_weekday, use_container_width=True)
    
    # Top senders
    st.subheader("Top Email Senders")
    top_senders = emails_df['from_address'].value_counts().head(10)
    fig_senders = px.bar(
        x=top_senders.values, y=top_senders.index,
        orientation='h',
        title='Top 10 Email Senders',
        labels={'x': 'Number of Emails', 'y': 'Sender Email'}
    )
    fig_senders.update_layout(height=400)
    st.plotly_chart(fig_senders, use_container_width=True)

def create_response_analytics(merged_df, analytics_df):
    """Create response analytics"""
    st.header("↩️ Response Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Response time distribution
        response_times = merged_df[merged_df['response_time_hours'].notna()]
        if not response_times.empty:
            # Convert to days for better visualization
            response_times_days = response_times['response_time_hours'] / 24
            fig_response_time = px.histogram(
                response_times_days, 
                title='Response Time Distribution',
                labels={'value': 'Response Time (Days)', 'count': 'Frequency'},
                nbins=30
            )
            st.plotly_chart(fig_response_time, use_container_width=True)
        else:
            st.info("No response time data available")
    
    with col2:
        # Response rate distribution
        if not analytics_df.empty:
            fig_response_rate = px.histogram(
                analytics_df, x='response_rate_percent',
                title='Response Rate Distribution',
                labels={'response_rate_percent': 'Response Rate (%)', 'count': 'Frequency'},
                nbins=20
            )
            st.plotly_chart(fig_response_rate, use_container_width=True)
        else:
            st.info("No analytics data available")
    
    # Thread length analysis
    st.subheader("Conversation Thread Analysis")
    col3, col4 = st.columns(2)
    
    with col3:
        # Thread length distribution
        if not analytics_df.empty:
            thread_lengths = analytics_df['total_emails'].value_counts().sort_index()
            fig_threads = px.bar(
                x=thread_lengths.index, y=thread_lengths.values,
                title='Thread Length Distribution',
                labels={'x': 'Number of Emails in Thread', 'y': 'Number of Conversations'}
            )
            st.plotly_chart(fig_threads, use_container_width=True)
        else:
            st.info("No thread data available")
    
    with col4:
        # User vs External emails in conversations
        if not analytics_df.empty:
            fig_user_external = px.scatter(
                analytics_df, 
                x='user_emails_count', 
                y='external_emails_count',
                size='total_emails',
                hover_data=['response_rate_percent'],
                title='User vs External Emails in Conversations',
                labels={
                    'user_emails_count': 'User Emails Count',
                    'external_emails_count': 'External Emails Count'
                }
            )
            st.plotly_chart(fig_user_external, use_container_width=True)
        else:
            st.info("No conversation data available")

def create_conversation_details(analytics_df, merged_df):
    """Create detailed conversation analysis"""
    st.header("💬 Conversation Details")
    
    if analytics_df.empty:
        st.info("No conversation analytics data available")
        return
    
    # Conversation summary table
    st.subheader("Top Conversations by Activity")
    
    # Add conversation start and end info
    conversation_summary = analytics_df.copy()
    conversation_summary['duration_days'] = (
        pd.to_datetime(conversation_summary['last_activity_date']) - 
        pd.to_datetime(conversation_summary['conversation_start_date'])
    ).dt.days
    
    # Display top conversations
    top_conversations = conversation_summary.nlargest(10, 'total_emails')[
        ['conversation_id', 'total_emails', 'user_emails_count', 'external_emails_count', 
         'response_rate_percent', 'duration_days', 'conversation_start_date', 'last_activity_date']
    ]
    
    st.dataframe(
        top_conversations,
        column_config={
            "conversation_id": "Conversation ID",
            "total_emails": st.column_config.NumberColumn("Total Emails", format="%d"),
            "user_emails_count": st.column_config.NumberColumn("Your Emails", format="%d"),
            "external_emails_count": st.column_config.NumberColumn("External Emails", format="%d"),
            "response_rate_percent": st.column_config.NumberColumn("Response Rate", format="%.1f%%"),
            "duration_days": st.column_config.NumberColumn("Duration (Days)", format="%d"),
            "conversation_start_date": st.column_config.DateColumn("Start Date"),
            "last_activity_date": st.column_config.DateColumn("Last Activity")
        },
        use_container_width=True
    )

def create_filters_sidebar(emails_df):
    """Create sidebar filters"""
    st.sidebar.header("Filters")
    
    if emails_df.empty:
        st.sidebar.info("No data available for filtering")
        return None, [], []
    
    # Date range filter - Fixed the date conversion logic
    try:
        # Get min and max dates and ensure they're proper date objects
        date_series = pd.to_datetime(emails_df['date'])
        min_date_dt = date_series.min()
        max_date_dt = date_series.max()
        
        # Convert to Python date objects
        if pd.notna(min_date_dt) and pd.notna(max_date_dt):
            min_date = min_date_dt.date() if hasattr(min_date_dt, 'date') else date.today() - timedelta(days=30)
            max_date = max_date_dt.date() if hasattr(max_date_dt, 'date') else date.today()
        else:
            # Fallback dates if conversion fails
            min_date = date.today() - timedelta(days=30)
            max_date = date.today()
            
    except Exception as e:
        # Fallback to safe default dates
        st.sidebar.warning(f"Date processing issue: {str(e)}. Using default date range.")
        min_date = date.today() - timedelta(days=30)
        max_date = date.today()
    
    date_range = st.sidebar.date_input(
        "Select Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )
    
    # Folder filter
    folders = emails_df['folder'].unique()
    selected_folders = st.sidebar.multiselect(
        "Select Folders",
        options=folders,
        default=folders
    )
    
    # Importance filter
    importance_levels = emails_df['importance'].unique()
    selected_importance = st.sidebar.multiselect(
        "Select Importance Levels",
        options=importance_levels,
        default=importance_levels
    )
    
    return date_range, selected_folders, selected_importance

def apply_filters(emails_df, date_range, selected_folders, selected_importance):
    """Apply filters to the dataframe"""
    if emails_df.empty:
        return emails_df
        
    filtered_df = emails_df.copy()
    
    # Apply date filter
    if date_range and len(date_range) == 2:
        # Ensure we're comparing date objects properly
        start_date, end_date = date_range
        filtered_df = filtered_df[
            (pd.to_datetime(filtered_df['date']).dt.date >= start_date) & 
            (pd.to_datetime(filtered_df['date']).dt.date <= end_date)
        ]
    
    # Apply folder filter
    if selected_folders:
        filtered_df = filtered_df[filtered_df['folder'].isin(selected_folders)]
    
    # Apply importance filter
    if selected_importance:
        filtered_df = filtered_df[filtered_df['importance'].isin(selected_importance)]
    
    return filtered_df

def main():
    """Main Streamlit application"""
    st.title("📧 Email Analytics Dashboard")
    st.markdown("---")
    
    # Load data
    try:
        with st.spinner("Loading email data..."):
            emails_df_raw, relationships_df, analytics_df = load_email_data()
            
            # Debug information - Show what was loaded from database
            st.info("📊 **Data Loading Summary:**")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Raw Emails Loaded", len(emails_df_raw))
            with col2:
                st.metric("Relationships Loaded", len(relationships_df))
            with col3:
                st.metric("Analytics Loaded", len(analytics_df))
            
            # Show sample data if available
            if not emails_df_raw.empty:
                with st.expander("🔍 Sample Raw Email Data (First 5 rows)"):
                    st.dataframe(emails_df_raw[['email_id', 'subject', 'from_address', 'received_datetime', 'folder']].head())
            else:
                st.error("❌ No emails found in email_group table!")
                st.info("**Troubleshooting steps:**")
                st.write("1. Check if your Airflow DAG ran successfully")
                st.write("2. Verify data exists: `SELECT COUNT(*) FROM email_group;`")
                st.write("3. Check database connection settings")
                return
            
            if not relationships_df.empty:
                with st.expander("🔗 Sample Relationships Data"):
                    st.dataframe(relationships_df.head())
            else:
                st.warning("⚠️ No data in email_relationships table")
                
            if not analytics_df.empty:
                with st.expander("📈 Sample Analytics Data"):
                    st.dataframe(analytics_df.head())
            else:
                st.warning("⚠️ No data in conversation_analytics table")
            
            # Process the data
            merged_df, analytics_df = process_email_data(emails_df_raw, relationships_df, analytics_df)
            
            st.metric("Processed Emails (after filtering)", len(merged_df))
        
        # Check if we have data after processing
        if merged_df.empty:
            st.error("❌ No email data found after processing. Please check your database.")
            return
            
        # Create filters
        date_range, selected_folders, selected_importance = create_filters_sidebar(merged_df)
        
        # Apply filters
        filtered_emails = apply_filters(merged_df, date_range, selected_folders, selected_importance)
        filtered_merged = merged_df[merged_df['email_id'].isin(filtered_emails['email_id'])]
        
        # Check if filtered data is empty
        if filtered_emails.empty:
            st.warning("⚠️ No data matches the selected filters. Please adjust your filter criteria.")
            return
        
        st.success(f"✅ Dashboard ready with {len(filtered_emails)} emails!")
        st.markdown("---")
        
        # Overview metrics
        create_overview_metrics(filtered_emails, analytics_df)
        st.markdown("---")
        
        # Create tabs for different views
        tab1, tab2, tab3 = st.tabs(["📥 Incoming Analytics", "↩️ Response Analytics", "💬 Conversations"])
        
        with tab1:
            create_incoming_analytics(filtered_emails, filtered_merged)
        
        with tab2:
            create_response_analytics(filtered_merged, analytics_df)
        
        with tab3:
            create_conversation_details(analytics_df, filtered_merged)
        
        # Raw data section (collapsible)
        with st.expander("📋 Raw Data Preview"):
            st.subheader("Email Data")
            st.dataframe(filtered_emails.head(100), use_container_width=True)
            
            if not analytics_df.empty:
                st.subheader("Analytics Data")
                st.dataframe(analytics_df.head(50), use_container_width=True)
    
    except Exception as e:
        st.error(f"❌ Error loading data: {str(e)}")
        st.info("**Please check:**")
        st.write("- Database connection settings")
        st.write("- Table existence (email_group, email_relationships, conversation_analytics)")
        st.write("- Airflow DAG execution status")
        
        # Show more detailed error information
        if st.checkbox("🐛 Show detailed error (for debugging)"):
            st.exception(e)

if __name__ == "__main__":
    main()
