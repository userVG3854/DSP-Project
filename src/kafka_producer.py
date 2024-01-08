import json
from kafka import KafkaProducer
import streamlit as st
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME
from weather_api import create_producer, fetch_weather_data
from dashboard import weather_dashboard

# Kafka producer function
def publish_messages_to_kafka(location):
    # Instantiate a Kafka producer
    producer = create_producer()
    weather_info = fetch_weather_data(location)

    if weather_info:
        # If weather_info is already a JSON string (bytes), pass it directly
        producer.send(KAFKA_TOPIC_NAME, weather_info)

        return weather_info

    return False

if __name__ == '__main__':
    weather_dashboard("producer")

    # Show input field
    location = st.text_input('Location')

    # Show action button
    action = st.button('Publish weather data to Kafka')

    if action:
        message = publish_messages_to_kafka(location)
        if message:
            st.success("Weather data published to Kafka successfully.")
        else:
            st.error("Failed to publish data to Kafka.")