import os
import logging
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Optional, Any

import dlt
from dlt.sources.helpers import requests

# Configure logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Constants
CITIES = [
    {"city": "Sydney", "lat": -33.8688, "lon": 151.2093},
    {"city": "Melbourne", "lat": -37.8136, "lon": 144.9631},
    {"city": "Brisbane", "lat": -27.4698, "lon": 153.0251},
    {"city": "Perth", "lat": -31.9505, "lon": 115.8605},
    {"city": "Adelaide", "lat": -34.9285, "lon": 138.6007},
    {"city": "Canberra", "lat": -35.2809, "lon": 149.1300},
    {"city": "Hobart", "lat": -42.8821, "lon": 147.3272},
    {"city": "Darwin", "lat": -12.4634, "lon": 130.8456},
]

API_KEY = os.getenv("WEATHER_API_KEY")
if not API_KEY:
    raise ValueError("WEATHER_API_KEY not found in environment variables")

def format_time(ts: int) -> str:
    """Format timestamp to HH:MM:SS string."""
    return datetime.fromtimestamp(ts).strftime("%H:%M:%S")

def get_weather_data(city_info: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Fetch weather data for a specific city from OpenWeatherMap API.
    
    Args:
        city_info: Dictionary containing city name, lat, and lon.
        
    Returns:
        Dictionary with processed weather data or None if fetch fails.
    """
    city = city_info["city"]
    lat = city_info["lat"]
    lon = city_info["lon"]
    
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}&units=metric"
    
    try:
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
        logger.error(f"Error fetching data for {city}: {e}")
        return None
    except Exception as e:
        logger.exception(f"Unexpected error processing {city}: {e}")
        return None

@dlt.resource(table_name="dlt_weather_data", write_disposition="merge", primary_key=["city", "date"], table_format="iceberg")
def weather_source(cities_list: List[Dict]):
    """
    DLT resource that yields weather data for a list of cities.
    Uses parallel execution for faster data fetching.
    """
    results = []
    
    for city_info in cities_list:
        data = get_weather_data(city_info)
        if data:
            results.append(data)
            
    if results:
        logger.info(f"Successfully retrieved data for {len(results)} cities")
        yield results
    else:
        logger.warning("No data retrieved from API calls")

@dlt.source
def weather_data_source():
    """DLT source for weather data"""
    yield weather_source(CITIES)

def handler(event, context):
    """Lambda handler function."""
    logger.info("Starting Weather ETL job")
    
    try:
        etl_pipeline = dlt.pipeline(
            pipeline_name="weather_etl",
            destination="athena",
            dataset_name="dlt_weather_dataset",
        )

        load_info = etl_pipeline.run(weather_data_source())
        logger.info(f"Load info: {load_info}")
        
        return {
            "statusCode": 200, 
            "body": str(load_info)
        }
        
    except Exception as e:
        logger.exception("ETL Job Failed")
        return {
            "statusCode": 500,
            "body": f"ETL Job Failed: {str(e)}"
        }
        