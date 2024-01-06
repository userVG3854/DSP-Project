# DSP-Project : Weather Data Analysis and Kafka Integration
Repository of the project for the DSP course.

This repository contains code for fetching real-time weather data from WeatherAPI.com, producing it to Kafka, and performing temperature analysis.

## Project Structure

- **`dashboard.ipynb`**: Streamlit dashboard for visualizing weather data.
- **`analysis.ipynb`**: Jupyter notebook for analyzing weather data and generating experimental plots.
- **`kafka_producer.py`**: Python script for producing weather data to Kafka.
- **`weather_api.py`**: Python module for interacting with the WeatherAPI.com API.
- **`geo_plots.py`**: Python module for generating geographical plots.
- **`environment.py`**: Configuration file containing Kafka broker and topic information.

## Usage

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/weather-data-analysis.git
   cd weather-data-analysis

2. Install dependencies:

    ```bash
    pip install -r requirements.txt
    
3. Run the Streamlit dashboard:

    ```bash
    streamlit run dashboard.ipynb

Explore the Jupyter notebooks (dashboard.ipynb and analysis.ipynb) for detailed analysis and code explanations.

## Contributors

Your Name (@yourusername)

## License

This project is licensed under the MIT License.
