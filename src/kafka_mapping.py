import json
import requests
import matplotlib.pyplot as plt
import matplotlib.image as mpimg
from matplotlib.animation import FuncAnimation
from mpl_toolkits.basemap import Basemap
from kafka import KafkaConsumer, KafkaProducer
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME, WEATHER_API_KEY
from matplotlib.colors import Normalize
from matplotlib.cm import ScalarMappable
from mpl_toolkits.axes_grid1 import make_axes_locatable
import numpy as np
from scipy.ndimage import gaussian_filter

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

# Initialize plot for streaming data focused on France
fig, ax = plt.subplots(figsize=(10, 5))
m = Basemap(projection='mill',llcrnrlat=41,urcrnrlat=51,llcrnrlon=-5,urcrnrlon=10,resolution='i', ax=ax)
m.drawcoastlines()
m.drawcountries()

# Create a color map
norm = Normalize(vmin=-10, vmax=25)
cmap = plt.cm.coolwarm
sm = ScalarMappable(norm=norm, cmap=cmap)
sm.set_array([])

# Add a colorbar
divider = make_axes_locatable(ax)
cax = divider.append_axes("right", size="5%", pad=0.1)
fig.colorbar(sm, cax=cax, orientation='vertical')

# Create a grid for the temperature data
lon, lat = np.meshgrid(np.linspace(-5, 10, 500), np.linspace(41, 51, 500))
temp_grid = np.full_like(lon, 5)  # Set the default temperature to 5 degrees

# Function to update the plot
def update_plot(frame):
    # Kafka consumer to consume data
    consumer = KafkaConsumer(KAFKA_TOPIC_NAME, bootstrap_servers=KAFKA_BROKER_URL,
                             value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    for message in consumer:
        weather_info = message.value
        lat_city = float(weather_info['location']['lat'])
        lon_city = float(weather_info['location']['lon'])
        temp = weather_info['current']['temp_c']
        icon_url = weather_info['current']['condition']['icon']
        
        # Update the temperature grid
        dist = np.sqrt((lon - lon_city)**2 + (lat - lat_city)**2)
        mask = dist < 1.0  # Update a zone within 1 degree of the city
        temp_grid[mask] = temp
        temp_grid = gaussian_filter(temp_grid, sigma=10)  # Smooth the temperature grid

        # Draw the temperature grid
        m.imshow(temp_grid, cmap=cmap, norm=norm, origin='lower', extent=[-5, 10, 41, 51])
        
        # Display weather icon
        icon_img = mpimg.imread('http:' + icon_url)
        ax.imshow(icon_img, extent=[lon_city-1, lon_city+1, lat_city-1, lat_city+1], origin='upper')
        
        break  # Process one message per frame

    return ax,

# Create animation
ani = FuncAnimation(fig, update_plot, interval=10000, blit=False)
plt.show()

