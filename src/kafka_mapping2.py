import json
import requests
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
from mpl_toolkits.basemap import Basemap
from kafka import KafkaConsumer, KafkaProducer
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME, WEATHER_API_KEY
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from matplotlib.offsetbox import OffsetImage, AnnotationBbox
import datetime

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

# Function to get the current date and time
def get_current_datetime():
    now = datetime.datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S")

# Initialize Basemap for streaming data focused on France
fig, ax = plt.subplots(figsize=(10, 5))

# Set latitude and longitude boundaries for France
llcrnrlat = 41.0  # Lower left corner latitude
urcrnrlat = 51.0  # Upper right corner latitude
llcrnrlon = -5.0  # Lower left corner longitude
urcrnrlon = 10.0  # Upper right corner longitude

map_plot = Basemap(projection='merc', llcrnrlat=llcrnrlat, urcrnrlat=urcrnrlat,
                   llcrnrlon=llcrnrlon, urcrnrlon=urcrnrlon, lat_ts=20, resolution='c')
map_plot.drawcoastlines()
map_plot.drawcountries()

# Colormap for temperature
norm_temp = Normalize(-10, 40)
cmap_temp = plt.get_cmap('coolwarm')
sm_temp = ScalarMappable(norm_temp, cmap_temp)

# Function to update the plot
def update_plot(frame):
    # Kafka consumer to consume data
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        weather_info = message.value
        lat = weather_info['location']['lat']
        lon = weather_info['location']['lon']
        temp = weather_info['current']['temp_c']
        icon_url = weather_info['current']['condition']['icon']

        x, y = map_plot(lon, lat)
        color_temp = sm_temp.to_rgba(temp)

        # Plot the temperature as a colored dot
        map_plot.plot(x, y, 'o', markersize=10, color=color_temp)

        # Display the weather icon
        imagebox = OffsetImage(plt.imread(icon_url), zoom=0.1)
        ab = AnnotationBbox(imagebox, (x, y), frameon=False)
        ax.add_artist(ab)

        break  # Process one message per frame

    # Add title with the region (France) and current date and time
    ax.set_title(f"France - {get_current_datetime()}", fontsize=14)

    return ax,

# Create animation
ani = FuncAnimation(fig, update_plot, interval=10000, blit=False)
plt.show()
