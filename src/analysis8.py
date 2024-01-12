import json
import requests
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.animation import FuncAnimation
import geopandas as gpd
from kafka import KafkaConsumer, KafkaProducer
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME, WEATHER_API_KEY

# Define a function to create a Kafka producer
def create_producer():
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL,
                             value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    return producer

# Function to fetch real-time weather data from the API
def fetch_weather_data(location):
    api_url = f"http://api.weatherapi.com/v1/current.json?key={WEATHER_API_KEY}&q={location}"
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to send weather data to Kafka
def send_weather_data(location):
    weather_data = fetch_weather_data(location)
    if weather_data:
        producer = create_producer()
        producer.send(KAFKA_TOPIC_NAME, weather_data)
        producer.flush()

# Load geospatial data
france = gpd.read_file('regions.geojson')

# Initialize plot for streaming data focused on France
fig, ax = plt.subplots(figsize=(10, 5))

# Function to update the plot
def update_plot(frame):
    # Kafka consumer to consume data
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        weather_info = message.value
        region = weather_info['location']['region']
        temp = weather_info['current']['temp_c']
        icon_url = weather_info['current']['condition']['icon']
        
        # Color the region based on temperature
        france.loc[france['region'] == region, 'temp'] = temp
        france.plot(column='temp', cmap='coolwarm', linewidth=0.8, ax=ax, edgecolor='0.8')
        
        # Display weather icon
        lat = weather_info['location']['lat']
        lon = weather_info['location']['lon']
        icon_img = mpimg.imread('http:' + icon_url)
        ax.imshow(icon_img, extent=[lon-1, lon+1, lat-1, lat+1], origin='upper')
        
        break  # Process one message per frame

    return ax,

# Create animation
ani = FuncAnimation(fig, update_plot, interval=10000, blit=False)
plt.show()
