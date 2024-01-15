import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, SGDRegressor
from sklearn.model_selection import train_test_split
import requests
import json
from datetime import datetime, timedelta
from kafka import KafkaConsumer
from environment import WEATHER_API_KEY, KAFKA_BROKER_URL, KAFKA_TOPIC_NAME
from dashboard import weather_dashboard


def fetch_weather_data(location, date):
    api_key = WEATHER_API_KEY
    response = requests.get(f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date}")
    return json.loads(response.text)


def process_weather_data(data):
    if 'forecast' in data and 'forecastday' in data['forecast'] and len(data['forecast']['forecastday']) > 0:
        df = pd.json_normalize(data['forecast']['forecastday'][0]['hour'])
        return df[['time', 'temp_c', 'humidity', 'wind_kph', 'pressure_mb']]
    else:
        return pd.DataFrame()


def perform_regression(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2,
                                                        random_state=42)
    model_batch = LinearRegression()
    model_batch.fit(X_train, y_train)
    batch_score = round(model_batch.score(X_test, y_test), 2)
    model_online = SGDRegressor()
    for i in range(len(X_train)):
        model_online.partial_fit([X_train.iloc[i]], [y_train.iloc[i]])
        online_score = round(model_online.score(X_test, y_test), 3)
    col1, col2 = st.columns(2)
    col1.metric(label="Batch Regression Score", value=batch_score)
    col2.metric(label="Online Regression Score", value=online_score)


def main():
    consumer = KafkaConsumer(
        KAFKA_TOPIC_NAME,
        bootstrap_servers=KAFKA_BROKER_URL,
        value_deserializer=lambda x: json.loads(x.decode("utf-8")),
    )

    for msg in consumer:
        col1, col2, col3, col4, col5 = st.columns(5)
        col3.metric("", msg.value['location']['name'],
                    msg.value['location']['country'])
        location = msg.value['location']['name']

        current_date = datetime.now()
        start_date = current_date - timedelta(days=7)
        end_date = current_date + timedelta(days=1)
        dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i
                 in range((end_date - start_date).days + 1)]

        dfs = []
        for date in dates:
            data = fetch_weather_data(location, date)
            df = process_weather_data(data)
            dfs.append(df)

        df = pd.concat(dfs, ignore_index=True)
        colors = ['blue', 'green', 'red', 'gold']
        colors *= (len(df.columns) // len(colors)) + 1

        fig, axes = plt.subplots(nrows=len(df.columns) - 1, figsize=(15, 10),
                                 sharex=True)

        for i, column in enumerate(df.columns[1:]):
            df.plot(x='time', y=column, ax=axes[i], color=colors[i])

        for ax in axes[:-1]:
            ax.set_xlabel('')
            ax.xaxis.set_tick_params(labelbottom=False)

        axes[-1].xaxis.set_tick_params(labelbottom=True)
        axes[-1].set_xlabel('Time')
        axes[-1].tick_params(axis='x', rotation=90)
        st.pyplot(fig)

        # Regression
        X = df[['humidity', 'wind_kph', 'pressure_mb']]
        y = df['temp_c']
        perform_regression(X, y)


if __name__ == '__main__':
    weather_dashboard("analysis")
    main()
