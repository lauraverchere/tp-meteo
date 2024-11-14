# Configuration
from kafka import KafkaProducer
import json
import time 

API_KEY = '60c351069138b06a8e6cc9b06d8c4752'  # Clé API
CITIES = ['Paris', 'London', 'Tokyo']  # Villes cibles


# Configuration
KAFKA_SERVER = 'localhost:9092'

# Initialisation du producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_SERVER],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for city in CITIES : 
  # Envoi des données en continu
  producer.send("tp-meteo",  value=city)

  import requests



# Fonction pour récupérer les données météo
def get_weather_data(city):
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    return response.json() if response.status_code == 200 else None




# Boucle d'envoi en continu
while True:
    for city in CITIES:
        weather_data = get_weather_data(city)
        
        if weather_data:
            # Extraire les données pertinentes
            data = {
                'city': city,
                'temp': weather_data['main']['temp'],
                'feels_like': weather_data['main']['feels_like'],
                'temp_min': weather_data['main']['temp_min'],
                'temp_max': weather_data['main']['temp_max'],
                'pressure': weather_data['main']['pressure'],
                'humidity': weather_data['main']['humidity'],
                'sea_level': weather_data['main']['sea_level'],
                'grnd_level': weather_data['main']['grnd_level']
            }
            
            # Envoi des données dans le topic Kafka 'tp-meteo'
            producer.send('tp-meteo', value=data)
            
            # Affichage du message de confirmation
            print(f"Les données météo pour {city} ont été envoyées avec succès.")
        else:
            print(f"Impossible de récupérer les données pour {city}.")
    
    # Attendre 60 secondes avant le prochain envoi
    time.sleep(60)

# Assurer que toutes les données sont envoyées avant de fermer le producteur
producer.flush()

# Fermeture du producteur après l'envoi
producer.close()