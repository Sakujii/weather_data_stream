import requests
import json
from datetime import datetime
import time

cities = ['Rovaniemi', 'Helsinki', 'Gdansk', 'Athens']

# Preparing empty list for JSON data
stripped_data = []

while True:
    for city in cities:

        source_url = 'http://api.openweathermap.org/data/2.5/weather?q={}&appid="MY_API_KEY"&units=metric'.format(city)
        push_url = 'MY_POWERBI_STREAMING_DATASET_URL'
        res = requests.get(source_url)

        data = res.json()

        # Converting UNIX timestamps to Power BI JSON format
        timestamp = datetime.fromtimestamp(data['dt']).strftime("%Y-%m-%dT%H:%M:%S")
        sunrise = datetime.fromtimestamp(data['sys']['sunrise']).strftime("%Y-%m-%dT%H:%M:%S")
        sunset = datetime.fromtimestamp(data['sys']['sunset']).strftime("%Y-%m-%dT%H:%M:%S")

        # Creating a new dict to be appended in the JSON
        city_data = {
            "city": city,
            "temperature": data['main']['temp'],
            "pressure": data['main']['pressure'],
            "humidity": data['main']['humidity'],
            "wind_speed": data['wind']['speed'],
            "wind_direction": data['wind']['deg'],
            "timestamp": timestamp,
            "sunrise": sunrise,
            "sunset": sunset,
            "latitude": data['coord']['lat'],
            "longitude": data['coord']['lon'],
            "description": data['weather'][0]['description'],
            "icon": data['weather'][0]['icon']
        }
        stripped_data.append(city_data)

    # Headers and response to Power BI streaming dataset

    response = requests.request(
        method = "POST",
        url = push_url,
        headers = {"Content-Type": "application/json"},
        data = json.dumps(stripped_data)
    )

    # Checking response status and printing accordingly
    if response.status_code == 200:
      print("Successfully fetched and deployed at", timestamp)
    else:
      print("Something went wrong!")

    time.sleep(3600)


# Variables and printing to console
"""
temperature = data['main']['temp']
pressure = data['main']['pressure']
humidity = data['main']['humidity']
wind_speed = data['wind']['speed']
wind_direction = data['wind']['deg']
timestamp = data['dt']
sunrise = data['sys']['sunrise']
sunset = data['sys']['sunset']
latitude = data['coord']['lat']
longitude = data['coord']['lon']
description = data['weather'][0]['description']

print('City : {}'.format(city))
print('Latitude : {}'.format(latitude))
print('Longitude : {}'.format(longitude))
print('Time : {}'.format(timestamp))
print('Sunrise : {}'.format(sunrise))
print('Sunset : {}'.format(sunset))

print('Temperature : {} deg celcius'.format(temperature))
print('Humidity : {} %'.format(humidity))
print('Wind speed : {} m/s'.format(wind_speed))
print('Wind direction : {} degrees'.format(wind_direction))
print('Pressure : {} hPa'.format(pressure))
print('Description : {}'.format(description))
"""