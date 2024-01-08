import geopandas as gpd
import matplotlib.pyplot as plt
from shapely.geometry import Point
from kafka import KafkaConsumer
import json
from sklearn.linear_model import SGDRegressor
from environment import KAFKA_BROKER_URL

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

def consume_weather_data(cities):
    # Function to consume weather data from Kafka and plot it on a world map
    consumer = KafkaConsumer(
        'weather_topic', # The topic name
        bootstrap_servers=KAFKA_BROKER_URL, # The bootstrap server URL
        auto_offset_reset="earliest", # Start from the beginning of the topic
        enable_auto_commit=True, # Commit the offsets automatically
        group_id="weather-group", # The consumer group id
        value_deserializer=lambda v: json.loads(v.decode("utf-8")) # A function to deserialize the messages as JSON
    )

    data = {'city': [], 'latitude': [], 'longitude': [], 'temperature': []}
    model = SGDRegressor()

    for message in consumer:
        if message is not None:
            weather_data = message.value  # Removed .decode('utf-8') and json.loads()
            if weather_data['location']['name'] in cities:
                data['city'].append(weather_data['location']['name'])
                data['latitude'].append(weather_data['location']['lat'])
                data['longitude'].append(weather_data['location']['lon'])
                data['temperature'].append(weather_data['current']['temp_c'])

                # Perform online linear regression
                X = [[weather_data['location']['lat'], weather_data['location']['lon']]]
                y = [weather_data['current']['temp_c']]
                model.partial_fit(X, y)

                # Plot updated weather data on a world map
                geo_df = create_geo_df(data)
                plot_weather_data(geo_df)


if __name__ == "__main__":
    cities = ['New York', 'London', 'Tokyo']  # Specify the cities studied
    # Uncomment the line below to continuously consume and plot real-time weather data from Kafka
    consume_weather_data(cities)
