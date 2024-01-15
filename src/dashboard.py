import streamlit as st

def weather_dashboard(dashboard_kind: str):
    # Set up default configurations
    st.set_page_config(
        page_title="Live Weather Data Application",
        page_icon="⛅",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # Launch the Streamlit application
    st.title(f"{dashboard_kind.upper()} : Weather Information")

    st.sidebar.image("../images/weatherapi.png", width=250)

    st.sidebar.markdown(
        """
        This application retrieves live weather data from WeatherAPI.com.
        It produces messages to a Kafka topic and consumes messages from the Kafka topic, 
        then displays live weather data from the Kafka messages.
        """
    )

    st.header("Live Weather Data with Kafka and Streamlit")
