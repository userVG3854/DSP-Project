from kafka import KafkaProducer
import json
from environment import (
    WEATHER_API_KEY,
    KAFKA_BROKER_URL
)

# Define a function to create a Kafka producer
def create_producer():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

# Define a function to send weather data to Kafka
def send_weather_data(producer, topic, location_key, loc_name, country):
    params = {
        "apikey": WEATHER_API_KEY,
        "details": "true",
    }
    w_api = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={location_key}"

    # Instead of sending an API request, we send the API URL to Kafka
    weather_data = {
        "api_url": w_api,
        "location": loc_name,
        "country": country,
    }
    producer.send(topic, weather_data)

# Define a function to fetch location key and send weather data
def fetch_and_send(location: str):
    loc_api = f"http://api.weatherapi.com/v1/search.json?key={WEATHER_API_KEY}&q={location}"

    # Instead of sending an API request, we send the API URL to Kafka
    producer = create_producer()
    topic = 'weather_topic'
    send_weather_data(producer, topic, loc_api, location)

