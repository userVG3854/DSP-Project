import json
import time
from kafka import KafkaConsumer
import streamlit as st
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME
from dashboard import weather_dashboard
from river import compose
from river import linear_model
from river import metrics
from river import preprocessing
from sklearn import linear_model as sk_linear_model
from sklearn import metrics as sk_metrics
from sklearn import preprocessing as sk_preprocessing
import geopandas as gpd
from shapely.geometry import Point
import matplotlib.pyplot as plt
import pandas as pd

# Initialize the online regression model
online_model = compose.Pipeline(
    preprocessing.StandardScaler(),
    linear_model.LinearRegression()
)

# Initialize the metric
online_metric = metrics.MAE()

# Initialize the data storage
data_storage = []

# Function to handle weather data from Kafka message
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

    # Use the weather data for online regression
    xi = {key: value for key, value in weather_info['current'].items() if isinstance(value, (int, float))}
    yi = weather_info['current']['temp_c']  # Replace this with your target variable
    y_pred = online_model.predict_one(xi)
    online_model.learn_one(xi, yi)
    online_metric.update(yi, y_pred)

    st.success(f"Online regression MAE: {online_metric.get()}")

    # Store the data for plotting
    data_storage.append({
        'city': weather_info['location']['name'],
        'latitude': weather_info['location']['lat'],
        'longitude': weather_info['location']['lon'],
        'temperature': weather_info['current']['temp_c']
    })

    # Plot the weather data on a world map
    data = pd.DataFrame(data_storage)
    geo_df = create_geo_df(data)
    plot_weather_data(geo_df)

# Function to create a GeoDataFrame from weather data
def create_geo_df(data):
    geometry = [Point(lon, lat) for lon, lat in zip(data['longitude'], data['latitude'])]
    geo_df = gpd.GeoDataFrame(data, geometry=geometry)
    return geo_df

def plot_weather_data(data):
    world = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    
    fig, ax = plt.subplots(figsize=(15, 10))
    world.plot(ax=ax, color='lightgray')

    data.plot(ax=ax, markersize=data['temperature']*2, alpha=0.7, cmap='coolwarm', legend=True)
    
    for idx, row in data.iterrows():
        plt.annotate(row['city'], (row['geometry'].x, row['geometry'].y), textcoords="offset points", xytext=(0, 5), ha='center')

    plt.title('Real-time Weather Data on World Map')
    plt.show()


def consume_messages_from_kafka(max_time=60):
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    start_time = time.time()
    for msg in consumer:
        if time.time() - start_time > max_time:
            break
        handle_weather_info(msg)


# Update the Streamlit app with data from the background thread
if __name__ == '__main__':
    # Streamlit dashboard
    weather_dashboard("consumer")

    # Consume Kafka messages
    consume_messages_from_kafka()
