import time
import streamlit as st
import pandas as pd
import plotly.express as px
from pymongo import MongoClient

# --------------------------------------------------
# PAGE CONFIGURATION
# --------------------------------------------------

st.set_page_config(
    page_title="OTT Streaming Analytics Dashboard",
    page_icon="📺",
    layout="wide"
)
st.markdown(
    "<meta http-equiv='refresh' content='5'>",
    unsafe_allow_html=True
)

st.title("📺 OTT Streaming Analytics Dashboard")
st.caption(
    "Analysis of OTT clickstream events consumed from Apache Kafka and stored in MongoDB Atlas"
)

# --------------------------------------------------
# MONGODB CONNECTION
# --------------------------------------------------

MONGO_URI = "mongodb+srv://sda_dashboard:kolkata700152@sda-cluster.f09jsry.mongodb.net/?appName=SDA-Cluster"

client = MongoClient(MONGO_URI)

db = client["ott_streaming"]
collection = db["clickstream_events"]

# --------------------------------------------------
# LOAD DATA
# --------------------------------------------------

data = list(
    collection.find(
        {},
        {"_id": 0}
    )
)

if len(data) == 0:
    st.warning("No streaming events are currently available.")
    st.stop()

df = pd.DataFrame(data)

# --------------------------------------------------
# REMOVE TEST / INVALID DOCUMENTS
# --------------------------------------------------

required_columns = [
    "userId",
    "timestamp",
    "eventType",
    "contentId",
    "position_sec"
]

# This removes the MongoDB test document we inserted earlier
# and protects the dashboard from incomplete records.
df = df.dropna(subset=required_columns)

if df.empty:
    st.warning("No valid OTT streaming events are available.")
    st.stop()

# --------------------------------------------------
# DATA PREPARATION
# --------------------------------------------------

# Kafka timestamps are Unix timestamps in seconds
df["timestamp"] = pd.to_datetime(
    df["timestamp"],
    unit="s",
    errors="coerce"
)

df = df.dropna(subset=["timestamp"])

# --------------------------------------------------
# KPI METRICS
# --------------------------------------------------

total_events = len(df)

distinct_users = df["userId"].nunique()

distinct_content = df["contentId"].nunique()

latest_event = df["timestamp"].max()

st.subheader("Live Stream Summary")

col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Total Events",
    f"{total_events:,}"
)

col2.metric(
    "Distinct Users",
    distinct_users
)

col3.metric(
    "Distinct Content Items",
    distinct_content
)

col4.metric(
    "Latest Event Time",
    latest_event.strftime("%d %b %H:%M")
)

st.divider()

# --------------------------------------------------
# CHART 1: EVENT VOLUME OVER TIME
# --------------------------------------------------

st.subheader("1. Event Volume Over Time")

event_volume = (
    df.set_index("timestamp")
      .resample("10s")
      .size()
      .reset_index(name="Event Count")
)

fig1 = px.line(
    event_volume,
    x="timestamp",
    y="Event Count",
    markers=True,
    title="Number of Events per 10-Second Interval"
)

fig1.update_layout(
    xaxis_title="Time",
    yaxis_title="Number of Events"
)

st.plotly_chart(
    fig1,
    use_container_width=True
)

st.caption(
    "Shows how many interaction events were consumed during each 10-second interval."
)

# --------------------------------------------------
# CHART 2: EVENT TYPE DISTRIBUTION
# --------------------------------------------------

st.subheader("2. Event Type Distribution")

event_type_counts = (
    df.groupby("eventType")
      .size()
      .reset_index(name="Event Count")
      .sort_values("Event Count", ascending=False)
)

fig2 = px.bar(
    event_type_counts,
    x="eventType",
    y="Event Count",
    title="Distribution of PLAY, PAUSE and SEEK Events"
)

fig2.update_layout(
    xaxis_title="Event Type",
    yaxis_title="Number of Events"
)

st.plotly_chart(
    fig2,
    use_container_width=True
)

st.caption(
    "Compares the number of PLAY, PAUSE and SEEK interaction events present in the consumed stream."
)

# --------------------------------------------------
# CHART 3: CONTENT INTERACTION VOLUME
# --------------------------------------------------

st.subheader("3. Content Interaction Volume")

content_counts = (
    df.groupby("contentId")
      .size()
      .reset_index(name="Interaction Events")
      .sort_values("Interaction Events", ascending=False)
)

fig3 = px.bar(
    content_counts,
    x="contentId",
    y="Interaction Events",
    title="Interaction Events by Content"
)

fig3.update_layout(
    xaxis_title="Content ID",
    yaxis_title="Number of Interaction Events"
)

st.plotly_chart(
    fig3,
    use_container_width=True
)

st.caption(
    "Shows which content items generated the highest number of recorded interaction events."
)

# --------------------------------------------------
# OPTIONAL CHART 4: USER ACTIVITY
# --------------------------------------------------

st.subheader("4. User Activity")

user_counts = (
    df.groupby("userId")
      .size()
      .reset_index(name="Event Count")
      .sort_values("Event Count", ascending=False)
)

fig4 = px.bar(
    user_counts,
    x="userId",
    y="Event Count",
    title="Interaction Events by User"
)

fig4.update_layout(
    xaxis_title="User ID",
    yaxis_title="Number of Events"
)

st.plotly_chart(
    fig4,
    use_container_width=True
)

st.caption(
    "Shows how many interaction events were generated by each user."
)

# --------------------------------------------------
# RECENT EVENTS TABLE
# --------------------------------------------------

st.divider()

st.subheader("Recent Consumed Events")

recent_events = (
    df.sort_values("timestamp", ascending=False)
      .head(15)
)

st.dataframe(
    recent_events,
    use_container_width=True
)
