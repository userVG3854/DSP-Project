import json
from kafka import KafkaProducer
import streamlit as st
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME
from weather_api import create_producer, send_weather_data, fetch_and_send
from dashboard import weather_dashboard

# Kafka producer function
def publish_messages_to_kafka(loc):
    # Instantiate a Kafka producer
    producer = create_producer()
    weather_info = fetch_and_send(loc)

    if weather_info:
        result = json.dumps(weather_info).encode("utf-8")

        # Publish weather data to Kafka topic
        send_weather_data(producer, KAFKA_TOPIC_NAME, weather_info['location']['name'], weather_info['location']['name'], weather_info['location']['country'])
        return result

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