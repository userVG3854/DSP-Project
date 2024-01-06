# kafka_producer.py

import time
import json
from kafka import KafkaProducer # Import the kafka-python class
from weather_api import fetch_realtime_weather, fetch_forecast, fetch_history
from environment import (
    KAFKA_BROKER,
    KAFKA_TOPIC_REALTIME, # Use the topic name for real-time weather data
    KAFKA_TOPIC_FORECAST, # Use the topic name for forecast data
    KAFKA_TOPIC_HISTORY # Use the topic name for history data
)

# Create a producer instance for the weather topic
producer_realtime = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER], # The bootstrap server URL
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # A function to serialize the messages as JSON
)

# Create a producer instance for the forecast topic
producer_forecast = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER], # The bootstrap server URL
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # A function to serialize the messages as JSON
)

# Create a producer instance for the history topic
producer_history = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER], # The bootstrap server URL
    value_serializer=lambda v: json.dumps(v).encode("utf-8") # A function to serialize the messages as JSON
)

def produce_weather_data(cities):
    while True:
        for city in cities:
            realtime_weather = fetch_realtime_weather(city)
            forecast = fetch_forecast(city)
            history = fetch_history(city, time.strftime('%Y-%m-%d'))

            if realtime_weather and forecast and history:
                message_realtime = {
                    'city': city,
                    'realtime': realtime_weather['current'],
                    'timestamp': int(time.time())
                }
                message_forecast = {
                    'city': city,
                    'forecast': forecast['forecast']['forecastday'],
                    'timestamp': int(time.time())
                }
                message_history = {
                    'city': city,
                    'history': history['forecast']['forecastday'],
                    'timestamp': int(time.time())
                }
                # Use the producer instances to send the messages to the topics
                producer_realtime.send(KAFKA_TOPIC_REALTIME, value=message_realtime) # Use the topic name as a string
                producer_forecast.send(KAFKA_TOPIC_FORECAST, value=message_forecast) # Use the topic name as a string
                producer_history.send(KAFKA_TOPIC_HISTORY, value=message_history) # Use the topic name as a string

                print(f"Produced weather data for {city}")

            # Flush the producers
            producer_realtime.flush()
            producer_forecast.flush()
            producer_history.flush()

            time.sleep(10)  # Fetch data every 10 seconds

if __name__ == "__main__":
    cities = ['New York', 'London', 'Tokyo']  # Add more cities if needed

    # Produce weather data to Kafka
    produce_weather_data(cities)
