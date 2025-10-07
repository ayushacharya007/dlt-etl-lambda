from datetime import datetime
import os
import dlt
import pandas as pd
from dlt.sources.helpers import requests
from dotenv import load_dotenv
from typing import List, Dict, Optional, Any, Union

# Get the directory containing this script
script_dir = os.path.dirname(os.path.abspath(__file__))
# Load environment variables from .env file in parent directory
load_dotenv(dotenv_path=os.path.join(script_dir, "..", ".env"))

def format_time(ts):
    dt = datetime.fromtimestamp(ts)
    return dt.strftime("%H:%M:%S")

cities = [
    {"city": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"city": "Melbourne", "lat": -37.8136, "lon": 144.9631},
    {"city": "Brisbane", "lat": -27.4698, "lon": 153.0251},
    {"city": "Perth", "lat": -31.9505, "lon": 115.8605},
    {"city": "Adelaide", "lat": -34.9285, "lon": 138.6007},
    {"city": "Canberra", "lat": -35.2809, "lon": 149.1300},
    {"city": "Hobart", "lat": -42.8821, "lon": 147.3272},
    {"city": "Darwin", "lat": -12.4634, "lon": 130.8456},
]

# # function to get data from the api
# @dlt.resource(name="weather_data", table_name="weather_data", write_disposition="append")
def getData(city: str, lat: float, lon: float) -> Optional[Dict[str, Any]]:
    try:
        API_KEY = os.getenv("WEATHER_API_KEY")
        if not API_KEY:
            print(f"Warning: WEATHER_API_KEY not found in environment variables")
            print(f"Current working directory: {os.getcwd()}")
            return None
        url = (
            f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
        )
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        return {
            "country": data.get('sys', {}).get('country'),
            "city": city,
            "date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "temperature": data.get('main', {}).get('temp'),
            "feels_like": data.get('main', {}).get('feels_like'),
            "minimum_temperature": data.get('main', {}).get('temp_min'),
            "maximum_temperature": data.get('main', {}).get('temp_max'),
            "humidity": data.get('main', {}).get('humidity'),
            "wind_speed": data.get('wind', {}).get('speed'),
            "sunrise": format_time(data.get('sys', {}).get('sunrise', 0)),
            "sunset": format_time(data.get('sys', {}).get('sunset', 0)),
        }
    except requests.RequestException as e:
        print(f"Error fetching data for {city}: {e}")
        return None


@dlt.resource(table_name="weather_data", write_disposition="append")
def create_dataframe(cities:List[Dict]):
    final_data = []
    if cities:
        for city_info in cities:
            data = getData(city_info["city"], city_info["lat"], city_info["lon"])
            if data:
                final_data.append(data)
    yield final_data


etl_pipeline = dlt.pipeline(
    pipeline_name="weather_etl",
    destination="postgres",
    dataset_name="weather"
)


load = etl_pipeline.run(create_dataframe(cities))

print(f"Load info: {load}")