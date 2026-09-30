# OTT Streaming Analytics Dashboard

## Project Overview

This project implements a real-time OTT clickstream analytics pipeline using Apache Kafka, MongoDB Atlas, Streamlit, and Plotly.

OTT user interaction events such as PLAY, PAUSE, and SEEK are produced to a Kafka topic. A Python Kafka consumer reads these events and stores them in MongoDB Atlas. The Streamlit dashboard retrieves the stored events and provides analytical metrics and visualizations.

## Data Flow

Producer → Apache Kafka → Consumer → MongoDB Atlas → Streamlit → Plotly Dashboard

## Kafka Topic

`ott.clickstream.raw`

## Event Fields

Each streaming event contains:

- `userId`
- `timestamp`
- `eventType`
- `contentId`
- `position_sec`

## Dashboard Metrics

The dashboard displays:

- Total Events
- Distinct Users
- Distinct Content Items
- Latest Event Time

## Dashboard Charts

1. Event Volume Over Time
2. Event Type Distribution
3. Content Interaction Volume
4. User Activity

A Recent Consumed Events table is also displayed.

## Technologies Used

- Apache Kafka
- Python
- MongoDB Atlas
- Streamlit
- Plotly
- Pandas
- PyMongo

## Project Files

- `producer.py` – Produces OTT clickstream events to Kafka.
- `consumer.py` – Consumes Kafka events and stores them in MongoDB Atlas.
- `dashboard.py` – Creates the Streamlit analytics dashboard.
- `requirements.txt` – Contains the required Python dependencies.


```bash
docker start broker
