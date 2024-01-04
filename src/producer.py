import time
import json
import requests
from pykafka import KafkaClient

# Kafka setup
kafka_host = 'localhost:9092'
kafka_topic = 'weather_data'
client = KafkaClient(hosts=kafka_host)
topic = client.topics[kafka_topic]

# WeatherAPI.com API key
api_key = 'YOUR_WEATHERAPI.COM_API_KEY'

def fetch_weather_data(city):
    base_url = 'http://api.weatherapi.com/v1/current.json'
    params = {
        'key': api_key,
        'q': city,
    }

    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Failed to fetch data for {city}: {response.status_code}")
        return None

def produce_weather_data(cities):
    producer = topic.get_sync_producer()

    while True:
        for city in cities:
            weather_data = fetch_weather_data(city)
            if weather_data:
                message = {
                    'city': city,
                    'temperature': weather_data['current']['temp_c'],
                    'description': weather_data['current']['condition']['text'],
                    'lat': weather_data['location']['lat'],
                    'lon': weather_data['location']['lon'],
                    'timestamp': int(time.time())
                }
                producer.produce(json.dumps(message).encode('utf-8'))

                # Update online regression model
                x = {'temperature': message['temperature']}
                y = message['timestamp']
                online_model = online_model.learn_one(x, y)

                print(f"Produced weather data for {city}")

            time.sleep(10)  # Fetch data every 10 seconds


