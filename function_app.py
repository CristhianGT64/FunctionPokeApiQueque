import io
import os
import azure.functions as func
import datetime
import json
import logging
import requests
from dotenv import load_dotenv
import pandas as pd
from azure.storage.blob import BlobServiceClient
import random

app = func.FunctionApp()
load_dotenv()

logging.basicConfig( level=logging.INFO )
logger = logging.getLogger(__name__)

DOMAIN = os.getenv("DOMAIN")
AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
BLOB_CONTAINER_NAME = os.getenv("BLOB_CONTAINER_NAME")
STORA_ACCOUNT_NAME = os.getenv("STORA_ACCOUNT_NAME")

@app.queue_trigger(
    arg_name="azqueue"
    , queue_name="requests"
    , connection="QueueAzureWebJobsStorage"
)
def QueueTriggerPokeReport(azqueue: func.QueueMessage):
    body = azqueue.get_body().decode('utf-8')
    record = json.loads(body)
    id = record[0]["id"]

    update_request( id , "inprogress" )


    request_info = get_request(id)

    pokemons = get_pokemons( request_info[0]["type"] )
    sample_size = request_info[0].get("sample_size")
    

    try:
        sample_size = int(sample_size) if sample_size is not None else None
    except (ValueError, TypeError):
        sample_size = None


        # Si hay sample_size válido y menor al total, muestrear
    total = len(pokemons)
    if sample_size is not None and 0 < sample_size < total:
        pokemons_to_process = random.sample(pokemons, sample_size)
        logger.info(
            f"Aplicando muestreo: sample_size={sample_size}, total={total}"
        )
    else:
        pokemons_to_process = pokemons
        logger.info(
            f"Sin muestreo: sample_size={sample_size}, total={total}"
        )

    pokemon_bytes = generate_csv_to_blob(pokemons_to_process)

    # pokemon_bytes = generate_csv_to_blob( pokemons )

    blob_name = f"poke_report_{id}.csv"
    upload_csv_to_blob( blob_name=blob_name, csv_data=pokemon_bytes )
    logger.info( f"Archivo {blob_name} se subio con exito" )

    url_completa = f"https://{STORA_ACCOUNT_NAME}.blob.core.windows.net/{BLOB_CONTAINER_NAME}/{blob_name}"
    update_request( id , "completed", url_completa )

def update_request( id: int, status: str, url: str = None ) -> dict:
    payload = {
        "status": status
        , "id": id
    }
    if url:
        payload["url"] = url

    reponse = requests.put( f"{DOMAIN}/api/request" , json=payload )
    return reponse.json()

def get_request(id: int) -> dict:
    reponse = requests.get( f"{DOMAIN}/api/request/{id}"  )
    return reponse.json()

def get_pokemons( type: str ) -> dict:
    pokeapi_url = f"https://pokeapi.co/api/v2/type/{type}"
    response = requests.get(pokeapi_url, timeout=3000)
    data = response.json()
    pokemon_entries = data.get("pokemon", [] )
    return [ p["pokemon"] for p in pokemon_entries ]

def get_pokemon_details( pokemon_url: str ) -> dict:
    try:
        response = requests.get( pokemon_url, timeout=30 )
        response.raise_for_status()
        data = response.json()

        stats = data.get("stats", [])
        hp = stats[0]["base_stat"] if len(stats) > 0 else None
        attack = stats[1]["base_stat"] if len(stats) > 1 else None
        defense = stats[2]["base_stat"] if len(stats) > 2 else None
        special_attack = stats[3]["base_stat"] if len(stats) > 3 else None
        special_defense = stats[4]["base_stat"] if len(stats) > 4 else None
        speed = stats[5]["base_stat"] if len(stats) > 5 else None

        abilities_data = data.get("abilities", [])
        abilities_list = [ a["ability"]["name"] for a in abilities_data ]
        abilities_str = ", ".join( abilities_list )

        return {
            "hp": hp
            , "attack": attack
            , "defense": defense
            , "special_attack": special_attack
            , "special_defense": special_defense
            , "speed": speed
            , "abilities": abilities_str
        }
    except Exception as e:
        logger.error( f"Error al obtener detalles de pokemon {pokemon_url} {e}" )
        return {
            "hp": None
            , "attack": None
            , "defense": None
            , "special_attack": None
            , "special_defense": None
            , "speed": None
            , "abilities": ""
        }


def generate_csv_to_blob( pokemon_list: list ) -> bytes:
    filas = []

    for pokemon in pokemon_list:
        name = pokemon.get("name")
        url = pokemon.get("url")

        detalles = get_pokemon_details( url )

        fila = {
            "name": name
            , "url": url
        }
        fila.update( detalles )
        filas.append( fila )

    df = pd.DataFrame( filas )

    columnas = [
        "name",
        "url",
        "hp",
        "attack",
        "defense",
        "special_attack",
        "special_defense",
        "speed",
        "abilities",
    ]
    df = df[columnas]

    output = io.StringIO()
    df.to_csv( output , index=False, encoding='utf-8' )
    csv_bytes = output.getvalue().encode('utf-8')
    output.close()
    return csv_bytes

def upload_csv_to_blob( blob_name: str, csv_data: bytes ):
    try:
        blob_service_client = BlobServiceClient.from_connection_string( AZURE_STORAGE_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client( container = BLOB_CONTAINER_NAME, blob=blob_name )
        blob_client.upload_blob( csv_data , overwrite=True )
    except Exception as e:
        logger.error(f"Error al subir el archivo {e} ")
        raise