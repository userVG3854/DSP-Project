import streamlit as st
import json
import requests
import matplotlib.pyplot as plt
import geopandas as gpd
from kafka import KafkaConsumer, KafkaProducer
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME, WEATHER_API_KEY
import matplotlib.colors as mcolors
from dashboard import weather_dashboard

def create_producer():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer


def fetch_weather_data(location):
    api_url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={location}"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Send weather data to Kafka
def send_weather_data(location):
    weather_data = fetch_weather_data(location)
    if weather_data:
        producer = create_producer()
        producer.send(KAFKA_TOPIC_NAME, weather_data)
        producer.flush()

def create_and_update_plot(consumer):
    # Load geospatial data for France
    france = gpd.read_file('images/regions.geojson')  
    france['temp'] = None 
    cmap = plt.cm.Reds
    placeholder = st.empty()
    data_points = []

    for message in consumer:
        weather_info = message.value
        lat = weather_info['location']['lat']
        lon = weather_info['location']['lon']
        temp = weather_info['current']['temp_c']
        norm = mcolors.Normalize(vmin=-10, vmax=10) 
        color = cmap(norm(temp))

        data_points.append((lon, lat, temp, color))

        # main plot
        fig, ax = plt.subplots(figsize=(10, 8))
        ax.set_facecolor('white')
        france.plot(ax=ax, color='white', edgecolor='gray')
        
        # Plot of the points
        for lon, lat, temp, color in data_points:
            ax.scatter(lon, lat, color=color, s=100)
            offset = 0.08
            ax.text(lon + offset, lat + offset, f'{temp}°C', color='black', fontsize=8, ha='left', va='bottom', fontweight='bold')

        ax.set_axis_off()

        # Update the plot in the Streamlit app
        placeholder.pyplot(fig)

def main():
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))

    create_and_update_plot(consumer)

if __name__ == '__main__':
    weather_dashboard("Visualization")
    main()

