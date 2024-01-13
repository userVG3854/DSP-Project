import pandas as pd
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, SGDRegressor
from sklearn.model_selection import train_test_split
import requests
import json
from datetime import datetime, timedelta
from environment import WEATHER_API_KEY

# Define a function to fetch the weather data for a specific date
def fetch_weather_data(location, date):
    api_key = WEATHER_API_KEY
    response = requests.get(f"http://api.weatherapi.com/v1/history.json?key={api_key}&q={location}&dt={date}")
    return json.loads(response.text)

# Define a function to process the weather data
def process_weather_data(data):
    df = pd.json_normalize(data['forecast']['forecastday'][0]['hour'])
    return df[['time', 'temp_c', 'humidity', 'wind_kph', 'pressure_mb']]

# Define a function to perform regression
def perform_regression(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model_batch = LinearRegression()
    model_batch.fit(X_train, y_train)
    print(f"Batch regression score: {model_batch.score(X_test, y_test)}")

    model_online = SGDRegressor()
    for i in range(len(X_train)):
        model_online.partial_fit([X_train.iloc[i]], [y_train.iloc[i]])
    print(f"Online regression score: {model_online.score(X_test, y_test)}")

# Define the location and the list of dates
location = "London"
current_date = datetime.now()
start_date = current_date - timedelta(days=7)
end_date = current_date + timedelta(days=1)
dates = [(start_date + timedelta(days=i)).strftime('%Y-%m-%d') for i in range((end_date - start_date).days + 1)]

# Fetch and process the weather data for each date
dfs = []
for date in dates:
    data = fetch_weather_data(location, date)
    df = process_weather_data(data)
    dfs.append(df)

# Concatenate the data from all dates
df = pd.concat(dfs, ignore_index=True)

# Plot the variables
df.plot(x='time', subplots=True)
plt.show()

# Perform regression to predict the temperature
X = df[['humidity', 'wind_kph', 'pressure_mb']]
y = df['temp_c']
perform_regression(X, y)
