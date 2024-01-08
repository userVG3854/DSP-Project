import json
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

# Initialize the online regression model
online_model = compose.Pipeline(
    preprocessing.StandardScaler(),
    linear_model.LinearRegression()
)

# Initialize the batch regression model
batch_model = sk_linear_model.LinearRegression()

# Initialize the metrics
online_metric = metrics.MAE()
batch_metric = sk_metrics.mean_absolute_error

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

    # Store the data for batch learning
    data_storage.append((xi, yi))

    # Use the stored data for batch learning
    X_train = [x for x, _ in data_storage]
    y_train = [y for _, y in data_storage]
    batch_model.fit(X_train, y_train)
    y_pred = batch_model.predict(X_train)
    batch_mae = batch_metric(y_train, y_pred)

    st.success(f"Batch regression MAE: {batch_mae}")

# Function to consume messages from Kafka topic
def consume_messages_from_kafka():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for msg in consumer:
        handle_weather_info(msg)

# Update the Streamlit app with data from the background thread
if __name__ == '__main__':
    # Streamlit dashboard
    weather_dashboard("consumer")

    # Consume Kafka messages
    consume_messages_from_kafka()
