import json 
import time 
import sys
from datetime import datetime
import requests
import os
from dotenv import load_dotenv
import shutil

shutil.copyfile('../producer/app/.env', '.env')

# Load environment variables from .env file
load_dotenv()

# Access your variable
api_key = os.getenv('API_KEY')

city_coordinates = {
    "Paris": (48.8566, 2.3522),
    "Marseille": (43.2965, 5.3698),
    "Lyon": (45.7578, 4.832),
    "Toulouse": (43.6045, 1.4442),
    "Nice": (43.7102, 7.262),
    "Nantes": (47.2184, -1.5536),
    "Montpellier": (43.6109, 3.8772),
    "Strasbourg": (48.5734, 7.7521),
    "Bordeaux": (44.8378, -0.5792),
    "Lille": (50.6292, 3.0573),
    "Brest": (48.3904, -4.4861),
    "Rennes": (48.1173, -1.6778)
}

def get_weather_data(lat, lon, api_key):
    url = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={api_key}&units=metric"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error occurred while fetching weather data for lat={lat} lon={lon}: {e}")
        return None

def append_to_json(data):
    with open('data.json', 'a') as json_file:
        json.dump(data, json_file)
        json_file.write('\n')

while True:
    for city, (lat, lon) in city_coordinates.items():
        weather_data = get_weather_data(lat, lon, api_key)
        if weather_data:
            weather_info = {
                'date': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "city": city,
                "lon": weather_data["coord"]['lon'],
                "lat": weather_data["coord"]['lat'],
                "temperature": weather_data["main"]['temp'],
                "feels_like": weather_data["main"]['feels_like'],
            }
            append_to_json(weather_info)
    time.sleep(30)  # Sleep for 2 seconds
