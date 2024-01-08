import sys
import json
import math
import time
from time import perf_counter
import pprint
import warnings
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import SGDRegressor
from sklearn.metrics import mean_squared_error
from kafka import KafkaProducer, KafkaConsumer
from river import compose, metrics, preprocessing, linear_model, optim
from environment import WEATHER_API_KEY, KAFKA_BROKER_URL

warnings.filterwarnings('ignore')

# Define weather features and target variable
weather_features = ['temp_c', 'humidity', 'wind_kph']
target_variable = 'precip_mm'

# Define the RIVER Regression model
model = compose.Pipeline(
    ('scale', preprocessing.StandardScaler()),
    ('lin_reg', linear_model.LinearRegression(intercept_lr=0, optimizer=optim.SGD(0.03)))
) >> preprocessing.TargetStandardScaler()

# Define Kafka Consumer function for getting weather data and running online machine learning
def evaluate_model(model, city):
    print(f"-----  ON-LINE MACHINE LEARNING FOR {city} ----")

    metric = metrics.MSE()
    topic_name = city
    topic_predict_name = f"predict__{city}"
    consumer_group_name = f"{city}_on_line_ML"

    consumer = KafkaConsumer(topic_name, bootstrap_servers=KAFKA_BROKER_URL, group_id=consumer_group_name)
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKER_URL)

    weather_data = {}
    nb_msg = 0
    result_to_store = False
    temps_cpu = 0

    # Collect a batch of data for batch learning
    X_batch, y_batch = [], []

    try:
        for message in consumer:
            weather_data = json.loads(message.value.decode())
            x = {feature: weather_data[feature] for feature in weather_features}
            y = weather_data[target_variable]

            if not math.isnan(y):
                nb_msg += 1

                temps = perf_counter()
                y_pred = model.predict_one(x)
                model.learn_one(x, y)
                temps_cpu += perf_counter() - temps

                MAE = metric.update(y, y_pred).get()

                if result_to_store:
                    predict_result = {
                        "timestamp_date": weather_data['timestamp_date'],
                        "y_true": y,
                        "y_pred": y_pred,
                        "CPU_time": temps_cpu,
                        "MAE": MAE
                    }
                    producer.send(topic_predict_name, json.dumps(predict_result).encode())
                else:
                    result_to_store = True

                # Add data to batch
                X_batch.append(list(x.values()))
                y_batch.append(y)

                if nb_msg == 5:
                    print("\n")
                    pprint.pprint(predict_result)
                    print("\n")

                if nb_msg % 50 == 0:
                    print(f"{time.strftime('%d/%m/%Y %H:%M:%S')} - {nb_msg} prediction values sent to the Kafka topic {topic_predict_name}")

    except KeyboardInterrupt:
        print(f"{time.strftime('%d/%m/%Y %H:%M:%S')} - {nb_msg} prediction values sent to the Kafka topic {topic_predict_name}")
        print("\n-------  END OF ON-LINE MACHINE LEARNING  -------")
    except Exception as e:
        print("An error has occurred")
        print(e)

    # Convert lists to numpy arrays for use with scikit-learn
    X_batch, y_batch = np.array(X_batch), np.array(y_batch)

    # Train a batch learning model
    batch_model = SGDRegressor().fit(X_batch, y_batch)

    # Make predictions and calculate error
    y_pred_batch = batch_model.predict(X_batch)
    mse = mean_squared_error(y_batch, y_pred_batch)

    print(f"Batch learning MSE: {mse}")

    # Plot comparison
    plt.figure(figsize=(10, 5))
    plt.plot(y_batch, label='True')
    plt.plot([y_pred for _, y_pred in model.predict_many(X_batch)], label='Online')
    plt.plot(y_pred_batch, label='Batch')
    plt.legend()
    plt.show()

# Example usage for a city (you can replace 'New York' with your desired city)
evaluate_model(model, 'New York')


