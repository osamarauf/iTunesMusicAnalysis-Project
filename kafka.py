import requests
from time import sleep
from confluent_kafka import Producer
from socket import gethostname
import json
conf = {'bootstrap.servers': "localhost:9092", 'client.id': gethostname()}
producer = Producer(conf)
kafka_topic = 'music'
top_artists = ['Radiohead', 'Thom Yorke',
               'Atoms for Peace', 'Jonny Greenwood', 'Philip Selway']
for artist in top_artists:
    url = f"https://itunes.apple.com/search?term={artist}"
    response = requests.get(url)
    data = response.json()
    results = data["results"]
    for result in results:
        track_name = result.get("trackName", "")
        artist_name = result.get("artistName", "")
        track_price = result.get("trackPrice", "")
        collection_price = result.get("collectionPrice", "")
        country = result.get("country", "")
        release_date = result.get("releaseDate", "")
        genre = result.get("primaryGenreName", "")
        track_data = {
            "artist_name": artist_name,
            "track_name": track_name,
            "track_price": track_price,
            "collectionPrice": collection_price,
            "country": country,
            "release_date": release_date,
            "genre": genre
        }
        track_json = json.dumps(track_data)
        producer.produce(kafka_topic, value=track_json)
