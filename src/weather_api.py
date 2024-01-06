import json
import time
from pykafka import KafkaClient
from kafka import KafkaProducer, KafkaConsumer # Import the kafka-python classes
from environment import (
    KAFKA_BROKER,
    KAFKA_TOPIC_REALTIME, # Use the topic name for real-time weather data
    KAFKA_TOPIC_FORECAST, # Use the topic name for forecast data
    KAFKA_TOPIC_HISTORY # Use the topic name for history data
)

# WeatherAPI.com API key
api_key = 'YOUR_WEATHERAPI.COM_API_KEY'

client = KafkaClient(hosts=KAFKA_BROKER)
topic_realtime = client.topics[KAFKA_TOPIC_REALTIME]
topic_forecast = client.topics[KAFKA_TOPIC_FORECAST]
topic_history = client.topics[KAFKA_TOPIC_HISTORY ]

# Create a producer instance
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER], # The bootstrap server URL
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # A function to serialize the messages as JSON
)

# Create a consumer instance
consumer = KafkaConsumer(
    KAFKA_TOPIC_REALTIME, # The topic name
    bootstrap_servers=[KAFKA_BROKER], # The bootstrap server URL
    auto_offset_reset="earliest", # Start from the beginning of the topic
    enable_auto_commit=True, # Commit the offsets automatically
    group_id="weather-group", # The consumer group id
    value_deserializer=lambda v: json.loads(v.decode("utf-8")) # A function to deserialize the messages as JSON
)

def produce_to_kafka(topic, data):
    # Produce data to Kafka topic
    producer.send(topic, value=data) # Use the producer instance to send the data

def fetch_realtime_weather(city):
    # Function to fetch real-time weather data using Kafka
    base_url = 'http://api.weatherapi.com/v1/current.json'
    params = {
        'key': api_key,
        'q': city,
    }

    # Fetch data from WeatherAPI.com
    message = {
        "url": base_url,
        "params": params
    }

    # Produce the message to the Kafka topic
    produce_to_kafka(KAFKA_TOPIC_REALTIME, message)

    # Consume the response from the Kafka topic
    for message in consumer:
        weather_data = message.value

        # Print some data
        print(f"Location: {weather_data['location']['name']}, {weather_data['location']['country']}")
        print(f"Current time: {weather_data['location']['localtime']}")
        print(f"Current temperature: {weather_data['current']['temp_c']} °C")
        print(f"Current condition: {weather_data['current']['condition']['text']}")

        return weather_data

def fetch_forecast(city):
    # Function to fetch forecast data using Kafka
    base_url = 'http://api.weatherapi.com/v1/forecast.json'
    params = {
        'key': api_key,
        'q': city,
        'days': 7
    }

    # Fetch data from WeatherAPI.com
    message = {
        "url": base_url,
        "params": params
    }

    # Produce the message to the Kafka topic
    produce_to_kafka(KAFKA_TOPIC_FORECAST, message)

    # Consume the response from the Kafka topic
    for message in consumer:
        forecast_data = message.value

        # Print some data
        print(f"Location: {forecast_data['location']['name']}, {forecast_data['location']['country']}")
        print(f"Forecast for {forecast_data['forecast']['forecastday'][0]['date']}:")
        print(f"Max temperature: {forecast_data['forecast']['forecastday'][0]['day']['maxtemp_c']} °C")
        print(f"Min temperature: {forecast_data['forecast']['forecastday'][0]['day']['mintemp_c']} °C")
        print(f"Condition: {forecast_data['forecast']['forecastday'][0]['day']['condition']['text']}")

        return forecast_data

def fetch_history(city, date):
    # Function to fetch history data using Kafka
    base_url = 'http://api.weatherapi.com/v1/history.json'
    params = {
        'key': api_key,
        'q': city,
        'dt': date
    }

    # Fetch data from WeatherAPI.com
    message = {
        "url": base_url,
        "params": params
    }

    # Produce the message to the Kafka topic
    produce_to_kafka(KAFKA_TOPIC_HISTORY, message)

    # Consume the response from the Kafka topic
    for message in consumer:
        history_data = message.value

        # Print some data
        print(f"Location: {history_data['location']['name']}, {history_data['location']['country']}")
        print(f"History for {history_data['forecast']['forecastday'][0]['date']}:")
        print(f"Max temperature: {history_data['forecast']['forecastday'][0]['day']['maxtemp_c']} °C")
        print(f"Min temperature: {history_data['forecast']['forecastday'][0]['day']['mintemp_c']} °C")
        print(f"Condition: {history_data['forecast']['forecastday'][0]['day']['condition']['text']}")

        return history_data

if __name__ == "__main__":
    cities = ['New York', 'London', 'Tokyo']  # Add more cities if needed

    # Fetch real-time, forecast, and history weather data for specified cities
    for city in cities:
        fetch_realtime_weather(city)
        fetch_forecast(city)
        fetch_history(city, '2022-01-05')  # Replace with the desired date

