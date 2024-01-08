import requests
from kafka import KafkaProducer
import json
from environment import WEATHER_API_KEY, KAFKA_BROKER_URL


def create_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER_URL,
        value_serializer=lambda v: v if isinstance(v, bytes) else json.dumps(v).encode('utf-8'))
    return producer

# Function to fetch weather data from the Weather API
def fetch_weather_data(location):
    try:
        response = requests.get(
            f"http://api.weatherapi.com/v1/current.json",
            params={"key": WEATHER_API_KEY, "q": location}
        )
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

