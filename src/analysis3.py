import sys
import json
import math
import time
from time import perf_counter
import pprint
import warnings
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error
from kafka import KafkaProducer, KafkaConsumer
from river import compose, metrics, preprocessing, linear_model, optim

warnings.filterwarnings('ignore')

# Define weather features and target variable
weather_features = ['temperature', 'humidity', 'wind_speed']  # Replace with actual weather features
target_variable = 'precipitation'  # Replace with actual target variable

# Define the RIVER Regression model
model = compose.Pipeline(
    ('scale', preprocessing.StandardScaler()),
    ('lin_reg', linear_model.LinearRegression(intercept_lr=0, optimizer=optim.SGD(0.03)))
)

model = preprocessing.TargetStandardScaler(regressor=model)

# Define Kafka Consumer function for getting weather data and running online machine learning
def evaluate_model(model, city):
    print("-----  ON-LINE MACHINE LEARNING FOR {} ----".format(city))

    metric = metrics.Rolling(metrics.MAE(), 7)
    topic_name = city
    topic_predict_name = "predict__{}".format(city)
    consumer_group_name = "{}_on_line_ML".format(city)

    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers='localhost:9092',
                             group_id=consumer_group_name)

    producer = KafkaProducer(bootstrap_servers="localhost:9092")

    weather_data = {}
    nb_msg = 0
    result_to_store = False
    temps_cpu = 0

    # Collect a batch of data for batch learning
    X_batch = []
    y_batch = []

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
                    print("")
                    pprint.pprint(predict_result)
                    print("")

                if nb_msg % 50 == 0:
                    print("{} - {} prediction values sent to the Kafka topic {}" \
                            .format(time.strftime("%d/%m/%Y %H:%M:%S"),
                                    nb_msg,
                                    topic_predict_name))
    except KeyboardInterrupt:
        print("{} - {} prediction values sent to the Kafka topic {}" \
                            .format(time.strftime("%d/%m/%Y %H:%M:%S"),
                                    nb_msg,
                                    topic_predict_name))
        print("\n-------  END OF ON-LINE MACHINE LEARNING  -------")
    except Exception as e:
        print("An error has occurred")
        print(e)

    # Convert lists to numpy arrays for use with scikit-learn
    X_batch = np.array(X_batch)
    y_batch = np.array(y_batch)

    # Train a batch learning model
    batch_model = LinearRegression()
    batch_model.fit(X_batch, y_batch)

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
