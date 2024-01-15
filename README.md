# DSP-Project : Weather Data Analysis and Kafka Integration
Repository of the project for the DSP course.

This repository contains code for fetching real-time weather data from WeatherAPI.com, producing it to Kafka, and performing temperature analysis.

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

What things you need to install the software and how to install them:

- Python 3.8 or higher
- Kafka

### Installing

A step by step series of examples that tell you how to get a development env running:

1. Clone the repository:

   ```bash
   git clone https://github.com/userVG3854/DSP-Project/
   cd DSP-Project

2. Install dependencies using:

    ```bash
    pip install -r requirements.txt

3. Start your Kafka server:

    ```bash
    # Start Zookeeper service
    bin/zookeeper-server-start.sh config/zookeeper.properties

    # Start Kafka server
    bin/kafka-server-start.sh config/server.properties

4. Run the producer script to produce weather data to Kafka:

    ```bash
    python -m streamlit run kafka_producer.py

5. Run the consumer script to consume weather data from Kafka and plot it on a french map:

    ```bash
    python -m streamlit run kafka_consumer.py
    python -m streamlit run kafka_mapping.py

6. Finally, plot different variables and try to implement regression methods:

    ```bash
    python -m streamlit run kafka_regression.py



Explore the scripts for detailed analysis and code explanations.

## Built With

* Python - The programming language used
* Kafka - Used for real-time data streaming
* Streamlit - Used to create the web application


## Project Structure

- **`dashboard.py`**: Streamlit dashboard for visualizing weather data.
- **`kafka_mapping.py`**, **`kafka_regression.py`**: Python script for analyzing weather data and generating experimental plots.
- **`kafka_producer.py`**: Python script for producing weather data to Kafka.
- **`weather_api.py`**: Python module for interacting with the WeatherAPI.com API.
- **`environment.py`**: Configuration file containing Kafka broker and topic information.

## Authors

* **Grimaud Vianney** - *Initial work* - @userVG3854

## License

This project is licensed under the MIT License.

## Acknowledgments

* Weather data provided by WeatherAPI.com
