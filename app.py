import pandas as pd
import requests
import time
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict

# Carregue sua chave de API de uma variável de ambiente para evitar exposição
api_key = 'GOOGLE_MAPS_API_KEY'

# Cache para armazenar geolocalizações já obtidas
cache = defaultdict(lambda: (None, None))

# Configuração do logging para registrar erros
logging.basicConfig(filename='geocodeERROR.log', level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def log_and_print(message):
    print(message)
    logging.info(message)

# Função para obter geolocalização usando a API do Google Maps
def pegar_geolocalizacao(cep, api_key):
    if cep in cache:
        return cache[cep]

    base_url = 'https://maps.googleapis.com/maps/api/geocode/json'
    parametros = {
        'address': f'{cep}, Brazil',
        'key': api_key
    }
    query = requests.get(base_url, params=parametros)
    if query.status_code == 200:
        arquivo_json = query.json()
        if arquivo_json['results']:
            localizacao = arquivo_json['results'][0]['geometry']['location']
            cache[cep] = (localizacao['lat'], localizacao['lng'])
            return localizacao['lat'], localizacao['lng']
        else:
            log_and_print(f"Nenhum resultado encontrado para o CEP {cep}")
            return None, None
    else:
        log_and_print(f"Erro na solicitação: {query.status_code}")
        return None, None

# Verifica se o arquivo CSV já existe e carrega os dados
if os.path.exists('resultado_geolocalizacao.csv'):
    df = pd.read_csv('resultado_geolocalizacao.csv')
    log_and_print("Arquivo parcial carregado.")
else:
    df = pd.read_csv('tabela_teste.csv', sep=';')
    df['latitude'] = None
    df['longitude'] = None
    log_and_print("Novo Arquivo carregado.")

# Inicializa cache de CEPs já processados
for i, row in df.iterrows():
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        cache[row['CEP']] = (row['latitude'], row['longitude'])

# Início do processo de geolocalização
tempo_inicio = datetime.now()
for i, row in df.iterrows():
    if pd.isna(row['latitude']) or pd.isna(row['longitude']):
        try:
            lat, lon = pegar_geolocalizacao(row['CEP'], api_key)
            log_and_print(f"CEP: {row['CEP']} -> Latitude: {lat}, Longitude: {lon}")
            df.at[i, 'latitude'] = lat
            df.at[i, 'longitude'] = lon
            time.sleep(3.6)
            if (i + 1) % 100 == 0:
                tempo_corrido = datetime.now() - tempo_inicio
                if tempo_corrido < timedelta(seconds=3):
                    tempo_esperando = (timedelta(seconds=3) - tempo_corrido).total_seconds()
                    log_and_print(f"Esperando {tempo_esperando} segundos para não sobrecarregar a API.")
                    time.sleep(tempo_esperando)
                tempo_inicio = datetime.now()
                df.to_csv('resultado_geolocalizacao.csv', index=False)
                log_and_print("Resultados parciais salvos no arquivo 'resultado_geolocalizacao.csv'")
        except Exception as e:
            log_and_print(f"Erro durante o processamento do CEP {row['CEP']}: {e}")

df.to_csv('resultado_geolocalizacao.csv', index=False)
log_and_print("Geolocalizações adicionadas com sucesso ao DataFrame.")
