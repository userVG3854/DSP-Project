import json
from kafka import KafkaConsumer
import matplotlib.pyplot as plt
from environment import KAFKA_BROKER_URL, KAFKA_TOPIC_NAME

# Function to handle weather data from Kafka message
def handle_weather_info(msg):
    # Extract the message value
    weather_info = msg.value

    # Extract the data you want to plot
    temperature = weather_info['current']['temp_c']
    humidity = weather_info['current']['humidity']

    return temperature, humidity

# Function to consume messages from Kafka topic
def consume_messages_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    temperatures = []
    humidities = []
    for msg in consumer:
        temperature, humidity = handle_weather_info(msg)
        temperatures.append(temperature)
        humidities.append(humidity)

        # Stop consuming after 100 messages
        if len(temperatures) >= 100:
            break

    return temperatures, humidities

# Consume Kafka messages
temperatures, humidities = consume_messages_from_kafka()

# Plot the data
plt.figure(figsize=(10, 5))
plt.plot(temperatures, label='Temperature (°C)')
plt.plot(humidities, label='Humidity (%)')
plt.xlabel('Time')
plt.ylabel('Value')
plt.title('Weather Data Over Time')
plt.legend()
plt.show()
