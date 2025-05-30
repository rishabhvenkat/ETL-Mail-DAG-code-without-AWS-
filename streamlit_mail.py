import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import altair as alt

# Set DB connection
DB_URI = "postgresql://<user>:<password>@<host>:<port>/<db_name>"
engine = create_engine(DB_URI)

st.set_page_config(layout="wide", page_title="Email Analytics Dashboard")

st.title("📧 Email Analytics Dashboard")

@st.cache_data
def load_email_data():
    query = "SELECT * FROM email_group;"
    df = pd.read_sql(query, engine)
    df['received_date'] = pd.to_datetime(df['received_date'])
    return df

df = load_email_data()

# Sidebar filters
with st.sidebar:
    st.header("📁 Filter Emails")
    folders = df['folder'].dropna().unique().tolist()
    selected_folders = st.multiselect("Select Folders", folders, default=folders)

    date_min, date_max = df['received_date'].min(), df['received_date'].max()
    selected_dates = st.date_input("Date Range", [date_min, date_max])

# Filtered dataframe
filtered_df = df[
    (df['folder'].isin(selected_folders)) &
    (df['received_date'] >= pd.to_datetime(selected_dates[0])) &
    (df['received_date'] <= pd.to_datetime(selected_dates[1]))
]

# 📈 Summary Stats
st.subheader("📊 Summary Stats")
col1, col2, col3 = st.columns(3)
col1.metric("Total Emails", len(filtered_df))
col2.metric("Unique Senders", filtered_df['from_email'].nunique())
col3.metric("Folders In View", len(selected_folders))

# 📅 Emails by Date
st.subheader("📆 Emails by Date")
by_date = filtered_df.groupby('received_date').size().reset_index(name='count')
chart = alt.Chart(by_date).mark_line(point=True).encode(
    x='received_date:T',
    y='count:Q'
).properties(width=800, height=400)
st.altair_chart(chart, use_container_width=True)

# 📂 Emails by Folder
st.subheader("📁 Emails by Folder")
folder_count = filtered_df.groupby('folder').size().reset_index(name='count')
st.bar_chart(folder_count.set_index('folder'))

# 🗃️ Raw Data Table
with st.expander("📄 View Raw Data"):
    st.dataframe(filtered_df)

