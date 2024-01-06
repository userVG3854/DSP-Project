import streamlit as st
import json
from kafka import KafkaConsumer
from environment import (
    KAFKA_BROKER,
    KAFKA_TOPIC
)


consumer = KafkaConsumer(KAFKA_TOPIC, bootstrap_servers=[KAFKA_BROKER], group_id='dashboard')

# Streamlit dashboard function
def dashboard():
    st.title("Weather Data Dashboard")

    # Display real-time weather data
    st.header("Real-time Weather Data")
    latest_message = get_latest_kafka_message()
    if latest_message:
        realtime_data = json.loads(latest_message.value)
        display_realtime_data(realtime_data)
    else:
        st.warning("No real-time weather data available.")

def get_latest_kafka_message():
    # Fetch the latest message from Kafka
    for message in consumer:
        pass  # Consume all messages to get the latest one
    return message

def display_realtime_data(realtime_data):
    st.write(f"City: {realtime_data['city']}")
    st.write(f"Temperature: {realtime_data['realtime']['temp_c']}°C")
    st.write(f"Condition: {realtime_data['realtime']['condition']['text']}")
    st.write(f"Wind Speed: {realtime_data['realtime']['wind_kph']} km/h")
    st.write(f"Humidity: {realtime_data['realtime']['humidity']}%")

if __name__ == '__main__':
    dashboard()
