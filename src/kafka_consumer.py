import json
from kafka import KafkaConsumer
import streamlit as st
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME
from dashboard import weather_dashboard

# Handle weather data from Kafka message
def handle_weather_info(msg):
    # Extract the message value
    weather_info = msg.value

    # Update the Streamlit app with the weather data
    col1, col2, col3 = st.columns(3)
    col1.metric("Location", weather_info['location']['name'], weather_info['location']['country'])
    col2.metric("Weather", weather_info['current']['condition']['text'])
    col3.metric("UV Index", weather_info['current']['uv'])

    col1.metric(label="Temperature", value=f"{weather_info['current']['temp_c']} °C")
    col2.metric(label="Feels Like", value=f"{weather_info['current']['feelslike_c']} °C")
    col3.metric("Wind", f"{weather_info['current']['wind_kph']} kph", f"{weather_info['current']['wind_dir']} direction")

    col1.metric("Precipitation", f"{weather_info['current']['precip_mm']} mm")
    col2.metric("Humidity", f"{weather_info['current']['humidity']}%")
    col3.metric("Cloud Cover", f"{weather_info['current']['cloud']}%")

    col1.metric("Wind Gust", f"{weather_info['current']['gust_kph']} kph")
    col2.metric("Pressure", f"{weather_info['current']['pressure_mb']} mb")
    col3.metric("Is Day", "Yes" if weather_info['current']['is_day'] == 1 else "No")

    st.success(f"Last updated: {weather_info['current']['last_updated']}")

# Consume messages from Kafka topic
def consume_messages_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for msg in consumer:
        handle_weather_info(msg)


if __name__ == '__main__':

    weather_dashboard("consumer")

    consume_messages_from_kafka()