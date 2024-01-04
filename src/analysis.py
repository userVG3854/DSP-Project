import time
import json
import requests
import pandas as pd
import matplotlib.pyplot as plt
from pykafka import KafkaClient
from river import linear_model
from sklearn.linear_model import LinearRegression

from src.producer import (
    fetch_weather_data,
    produce_weather_data
)

# Kafka setup
kafka_host = 'localhost:9092'
kafka_topic = 'weather_data'
client = KafkaClient(hosts=kafka_host)
topic = client.topics[kafka_topic]

# Online regression model using RIVER
online_model = linear_model.LinearRegression()

# Batch regression model using scikit-learn
batch_model = LinearRegression()

# WeatherAPI.com API key
api_key = 'YOUR_WEATHERAPI.COM_API_KEY'

def perform_batch_regression(X, y):
    # Perform batch regression using scikit-learn
    batch_model.fit(X, y)

def plot_geospatial_analysis(cities):
    latitudes, longitudes, temperatures = [], [], []

    for city in cities:
        weather_data = fetch_weather_data(city)
        if weather_data:
            latitudes.append(weather_data['location']['lat'])
            longitudes.append(weather_data['location']['lon'])
            temperatures.append(weather_data['current']['temp_c'])

    # Plot geospatial analysis
    plt.scatter(longitudes, latitudes, c=temperatures, cmap='viridis', s=100, edgecolors='k')
    plt.colorbar(label='Temperature (Celcius)')
    plt.xlabel('Longitude')
    plt.ylabel('Latitude')
    plt.title('Geospatial Analysis of Temperature')
    plt.show()

if __name__ == "__main__":
    cities = ['New York', 'London', 'Tokyo']  # Add more cities if needed

    # Bonus: Perform geospatial analysis and plotting
    plot_geospatial_analysis(cities)

    # Produce weather data to Kafka and update online regression model
    produce_weather_data(cities)

    # Example batch regression with scikit-learn
    # Replace X and y with actual features and target variables
    X = [[data['temperature']] for data in online_model]
    y = [data['timestamp'] for data in online_model]
    perform_batch_regression(X, y)

    # Perform comparisons and discussions of online vs batch regression
    # Add your comparison and discussion code here
