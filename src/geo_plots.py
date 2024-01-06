# geo_plots.py

import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from pykafka import KafkaClient
import json
from weather_api import fetch_realtime_weather

def create_geo_df(data):
    # Function to create a GeoDataFrame from weather data
    geometry = [Point(lon, lat) for lon, lat in zip(data['longitude'], data['latitude'])]
    geo_df = gpd.GeoDataFrame(data, geometry=geometry)
    return geo_df

def plot_weather_data(data):
    # Function to plot weather data on a world map
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    fig, ax = plt.subplots(figsize=(15, 10))
    world.plot(ax=ax, color='lightgray')

    data.plot(ax=ax, markersize=data['temperature']*2, alpha=0.7, cmap='coolwarm', legend=True)
    
    for idx, row in data.iterrows():
        plt.annotate(row['city'], (row['geometry'].x, row['geometry'].y), textcoords="offset points", xytext=(0, 5), ha='center')

    plt.title('Real-time Weather Data on World Map')
    plt.show()

def consume_weather_data():
    # Function to consume weather data from Kafka and plot it on a world map
    client = KafkaClient(hosts='localhost:9092')
    topic = client.topics['weather_data']
    consumer = topic.get_simple_consumer()

    data = {'city': [], 'latitude': [], 'longitude': [], 'temperature': []}

    for message in consumer:
        if message is not None:
            weather_data = json.loads(message.value.decode('utf-8'))
            data['city'].append(weather_data['city'])
            data['latitude'].append(weather_data['realtime']['latitude'])
            data['longitude'].append(weather_data['realtime']['longitude'])
            data['temperature'].append(weather_data['realtime']['temp_c'])

            # Plot updated weather data on a world map
            geo_df = create_geo_df(data)
            plot_weather_data(geo_df)

if __name__ == "__main__":
    # Uncomment the line below to continuously consume and plot real-time weather data from Kafka
    # consume_weather_data()
    pass  # Add the consumer code here when you want to consume and plot real-time data

