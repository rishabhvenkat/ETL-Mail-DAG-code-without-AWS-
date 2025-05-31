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

@st.cache_data
def load_email_data():
    """Load main email data"""
    query = "SELECT * FROM email_group;"
    df = pd.read_sql(query, engine)
    df['received_datetime'] = pd.to_datetime(df['received_datetime'])
    if 'sent_datetime' in df.columns:
        df['sent_datetime'] = pd.to_datetime(df['sent_datetime'])
    return df

@st.cache_data
def load_response_data():
    """Load email relationships data"""
    query = "SELECT * FROM email_relationships;"
    df = pd.read_sql(query, engine)
    return df

@st.cache_data
def load_conversation_analytics():
    """Load conversation analytics data"""
    query = "SELECT * FROM conversation_analytics;"
    df = pd.read_sql(query, engine)
    df['conversation_start_date'] = pd.to_datetime(df['conversation_start_date'])
    df['last_activity_date'] = pd.to_datetime(df['last_activity_date'])
    return df

@st.cache_data
def load_combined_data():
    """Load combined email and response data"""
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
    df['received_datetime'] = pd.to_datetime(df['received_datetime'])
    if 'sent_datetime' in df.columns:
        df['sent_datetime'] = pd.to_datetime(df['sent_datetime'])
    return df

try:
    # Load all data
    df = load_email_data()
    response_df = load_response_data()
    conversation_df = load_conversation_analytics()
    combined_df = load_combined_data()
    
    # Sidebar filters
    with st.sidebar:
        st.header("🔍 Filter Emails")
        
        # Folder filter
        folders = df['folder'].dropna().unique().tolist()
        selected_folders = st.multiselect("Select Folders", folders, default=folders)
        
        # Date filter
        date_min, date_max = df['received_datetime'].min(), df['received_datetime'].max()
        selected_dates = st.date_input("Date Range", [date_min.date(), date_max.date()])
        
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
    filtered_df = combined_df[
        (combined_df['folder'].isin(selected_folders)) &
        (combined_df['received_datetime'] >= pd.to_datetime(selected_dates[0])) &
        (combined_df['received_datetime'] <= pd.to_datetime(selected_dates[1]))
    ].copy()
    
    if show_responses_only:
        filtered_df = filtered_df[filtered_df['is_response'] == True]
    
    if show_user_emails_only:
        filtered_df = filtered_df[filtered_df['is_from_user'] == True]
    
    # Main Dashboard
    st.header("📊 Overview Metrics")
    
    # Summary Stats
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Emails", len(filtered_df))
    
    with col2:
        st.metric("Unique Senders", filtered_df['from_address'].nunique())
    
    with col3:
        if not conversation_df.empty:
            avg_response_rate = conversation_df['response_rate_percent'].mean()
            st.metric("Avg Response Rate", f"{avg_response_rate:.1f}%")
        else:
            st.metric("Avg Response Rate", "N/A")
    
    with col4:
        user_responses = filtered_df[(filtered_df['is_response'] == True) & (filtered_df['is_from_user'] == True)]
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
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Emails by Date
            by_date = filtered_df.groupby(filtered_df['received_datetime'].dt.date).size().reset_index(name='count')
            by_date.columns = ['date', 'count']
            chart = alt.Chart(by_date).mark_line(point=True).encode(
                x=alt.X('date:T', title='Date'),
                y=alt.Y('count:Q', title='Number of Emails'),
                tooltip=['date:T', 'count:Q']
            ).properties(width=400, height=300, title="Emails Received Over Time")
            st.altair_chart(chart, use_container_width=True)
        
        with col2:
            # Emails by Folder
            folder_count = filtered_df.groupby('folder').size().reset_index(name='count')
            chart = alt.Chart(folder_count).mark_bar().encode(
                x=alt.X('count:Q', title='Number of Emails'),
                y=alt.Y('folder:N', title='Folder', sort='-x'),
                color=alt.Color('folder:N', legend=None),
                tooltip=['folder:N', 'count:Q']
            ).properties(width=400, height=300, title="Emails by Folder")
            st.altair_chart(chart, use_container_width=True)
        
        # Top Senders
        st.subheader("Top Email Senders")
        top_senders = filtered_df.groupby('from_address').size().reset_index(name='count').sort_values('count', ascending=False).head(10)
        chart = alt.Chart(top_senders).mark_bar().encode(
            x=alt.X('count:Q', title='Number of Emails'),
            y=alt.Y('from_address:N', title='Sender Email', sort='-x'),
            tooltip=['from_address:N', 'count:Q']
        ).properties(height=400, title="Most Active Email Senders")
        st.altair_chart(chart, use_container_width=True)
    
    with tab2:
        st.subheader("⚡ Response Analytics")
        
        if not response_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Response Time Distribution
                user_responses = filtered_df[
                    (filtered_df['is_response'] == True) & 
                    (filtered_df['is_from_user'] == True) & 
                    (filtered_df['response_time_hours'].notna())
                ]
                
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
                if not conversation_df.empty:
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
            
            # Response Time Trends
            st.subheader("Response Time Trends")
            if not user_responses.empty:
                response_trends = user_responses.groupby(user_responses['received_datetime'].dt.date)['response_time_hours'].agg(['mean', 'median', 'count']).reset_index()
                response_trends.columns = ['date', 'mean', 'median', 'count']
                
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                
                fig.add_trace(
                    go.Scatter(x=response_trends['date'], y=response_trends['mean'], 
                              mode='lines+markers', name='Average Response Time'),
                    secondary_y=False,
                )
                
                fig.add_trace(
                    go.Bar(x=response_trends['date'], y=response_trends['count'], 
                          name='Number of Responses', opacity=0.3),
                    secondary_y=True,
                )
                
                fig.update_xaxes(title_text="Date")
                fig.update_yaxes(title_text="Response Time (Hours)", secondary_y=False)
                fig.update_yaxes(title_text="Number of Responses", secondary_y=True)
                fig.update_layout(title="Response Time and Volume Trends", height=400)
                
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No response time trend data available")
        else:
            st.info("No response analytics data available. Make sure the DAG has run and populated the email_relationships table.")
    
    with tab3:
        st.subheader("💬 Conversation Insights")
        
        if not conversation_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Conversation Length Distribution
                fig = px.histogram(
                    conversation_df, 
                    x='total_emails', 
                    nbins=15,
                    title="Conversation Length Distribution",
                    labels={'total_emails': 'Emails per Conversation', 'count': 'Number of Conversations'}
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # User Participation Rate
                fig = px.scatter(
                    conversation_df, 
                    x='total_emails', 
                    y='response_rate_percent',
                    size='user_emails_count',
                    hover_data=['external_emails_count'],
                    title="Response Rate vs Conversation Length",
                    labels={
                        'total_emails': 'Total Emails in Conversation',
                        'response_rate_percent': 'Response Rate (%)',
                        'user_emails_count': 'Your Emails'
                    }
                )
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Top Performing Conversations
            st.subheader("Most Active Conversations")
            top_conversations = conversation_df.nlargest(10, 'total_emails')[
                ['conversation_id', 'total_emails', 'user_emails_count', 'response_rate_percent', 'conversation_start_date']
            ]
            st.dataframe(top_conversations, use_container_width=True)
        else:
            st.info("No conversation analytics data available.")
    
    with tab4:
        st.subheader("📋 Detailed Response Analysis")
        
        # Response Metrics Summary
        if not response_df.empty:
            col1, col2, col3 = st.columns(3)
            
            with col1:
                st.metric("Total Responses Tracked", len(response_df[response_df['is_response'] == True]))
                st.metric("Your Responses", len(response_df[(response_df['is_response'] == True) & (response_df['is_from_user'] == True)]))
            
            with col2:
                if not conversation_df.empty:
                    st.metric("Conversations with 100% Response Rate", len(conversation_df[conversation_df['response_rate_percent'] == 100]))
                    st.metric("Conversations with 0% Response Rate", len(conversation_df[conversation_df['response_rate_percent'] == 0]))
            
            with col3:
                user_responses = response_df[(response_df['is_response'] == True) & (response_df['is_from_user'] == True)]
                if not user_responses.empty and 'response_time_hours' in user_responses.columns:
                    fast_responses = len(user_responses[user_responses['response_time_hours'] <= 1])
                    st.metric("Responses Within 1 Hour", fast_responses)
                    slow_responses = len(user_responses[user_responses['response_time_hours'] > 24])
                    st.metric("Responses After 24+ Hours", slow_responses)
        
        # Thread Analysis
        st.subheader("Email Thread Analysis")
        if not filtered_df.empty and 'thread_length' in filtered_df.columns:
            thread_analysis = filtered_df.groupby('thread_length').agg({
                'email_id': 'count',
                'response_time_hours': 'mean'
            }).reset_index()
            thread_analysis.columns = ['Thread Length', 'Number of Emails', 'Avg Response Time (Hours)']
            
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            fig.add_trace(
                go.Bar(x=thread_analysis['Thread Length'], y=thread_analysis['Number of Emails'], 
                      name='Email Count', opacity=0.7),
                secondary_y=False,
            )
            
            fig.add_trace(
                go.Scatter(x=thread_analysis['Thread Length'], y=thread_analysis['Avg Response Time (Hours)'], 
                          mode='lines+markers', name='Avg Response Time', line=dict(color='red')),
                secondary_y=True,
            )
            
            fig.update_xaxes(title_text="Thread Length (Number of Emails)")
            fig.update_yaxes(title_text="Number of Emails", secondary_y=False)
            fig.update_yaxes(title_text="Average Response Time (Hours)", secondary_y=True)
            fig.update_layout(title="Email Count and Response Time by Thread Length", height=400)
            
            st.plotly_chart(fig, use_container_width=True)
    
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
    st.error(f"Could not load data from the database.\n\nError: {e}")
    st.info("Make sure your database is running and the enhanced DAG has been executed to populate all tables.")
    
    # Debug information
    with st.expander("Debug Information"):
        st.write("Expected tables:")
        st.write("- email_group (main emails)")
        st.write("- email_relationships (response tracking)")  
        st.write("- conversation_analytics (conversation metrics)")
        st.write(f"Connection string: {DB_URI}")
        st.write(f"Error details: {str(e)}")
        
        # Show actual table structure if possible
        try:
            tables_query = """
            SELECT table_name, column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name IN ('email_group', 'email_relationships', 'conversation_analytics')
            ORDER BY table_name, ordinal_position;
            """
            schema_df = pd.read_sql(tables_query, engine)
            st.write("Current database schema:")
            st.dataframe(schema_df)
        except:
            st.write("Could not retrieve database schema")
