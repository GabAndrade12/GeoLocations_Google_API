import pandas as pd
import requests
import time
import logging
import os
from datetime import datetime, timedelta
from collections import defaultdict


api_key = 'AIzaSyCIiQ20rAGkqDnvdq2NholGmxImynHL30s'
cache = defaultdict(lambda:(None, None)) # Para caso acesse uma chave que não existe, ele não trava o codigo, cria uma chave padrão e continua o código

# Loggin caso ocorra um erro, para ficar mais fácil de localizar e resolver

logging.basicConfig(filename='geocodeERROR.log', level=logging.INFO, format='%(asctime)s: %(levelname)s: %(message)s')

def log_and_print(message):
    print(message)
    logging.info(message)

# Função para Achar o endereço com a API
def pegar_geolocalizacao(cep, api_key):
    if cep in cache:    # Verifica se o CEP está presente no dicionario cache
        return cache[cep] # Se o Cep está no dicionario retorna a chave que já estava armazenada antes
    
    base_url = 'https://maps.googleapis.com/maps/api/geocode/json'
    parametros = {
        'address': f'{cep}, Brazil',
        'key': api_key
    }
    query = requests.get(base_url, params=parametros)
    if query.status_code == 200:         # Verificação para ver se retornou algum valor da API
        arquivo_json = query.json()              # Converte a requisição da API em um formato json (texto)
        if arquivo_json['results']:                 # Caso tenha resultado na requisição vai rodar o bloco
            localizacao = arquivo_json['results'][0]['geometry']['location']   #Pega o primeiro resultado da geolocalização
            cache[cep] = (localizacao['lat'], localizacao['lng']) # Armazena lat e lng para fazer a verificação no cache
            return localizacao['lat'], localizacao['lng']                 #Retorna a latitude e longitude
        else:
            log_and_print(f"Nenhum resultado encontrado para o CEP {cep}")
            return None, None
    else:
        log_and_print(f"Erro na solicitação: {query.status_code}")
        return None, None

# Arquivo CSV, verifica se já tem algum dado nesses arquivos, caso tenha ele carrega o que já tinha antes.
if os.path.exists('resultado_geolocalizacao.csv'):
    df = pd.read_csv('resultado_geolocalizacao.csv')
    log_and_print("Arquivo parcial carregado.")

# Se não ouver nenhum dado carregado, ele lê a tabela desde o início e começa o processo
else:
    df = pd.read_csv('tabela_teste.csv', sep=';')
    df['latitude'] = None   # Adiciona uma coluna Latitude com todos valores None
    df['longitude'] = None  # Adiciona uma coluna Latitude com todos valores None
    log_and_print("Novo Arquivo carregado.")

    # Inicializar cache de CEPs já processados

for i, row in df.iterrows(): # Verifica linha por linha para ver se tem alguma celula vázia, se estiver preenchida adiciona ao cache para não ser feita a consulta novamente
    if pd.notna(row['latitude']) and pd.notna(row['longitude']):
        cache[row['CEP']] = (row['latitude'], row['longitude'])

# Inicio do tempo do processo
tempo_inicio = datetime.now()

for i, row in df.iterrows():    # Percorre cada linha do DataFrame e formando Tuplas(Pares)
    if pd.isna(row['latitude']) or pd.isna(row['longitude']): # Método que o pandas verifica se um valor é NaN(Not a Number)
        try:
            lat, lon = pegar_geolocalizacao(row['CEP'], api_key) # Chama a função da API e armazena nas variaveis, lat e lon
            log_and_print(f"CEP: {row['CEP']} -> Latitude: {lat}, Longitude: {lon}")
            df.at[i, 'Latitude'] = lat # Atualiza a célula da coluna latitude na linha i com o valor de lat
            df.at[i, 'Longitude'] = lon # Faz o mesmo com a coluna longitude
            time.sleep(3.6) # Pausa de 3.6 Segundos para não sobrecarregar a API

            if(i + 1) % 100 == 0: # A Cada X Quantidades de linha ele salva o arquivo CSV para não ocorrer perda de dados
                tempo_corrido = datetime.now() - tempo_inicio     #Pega o tempo de inicio e o tempo que passou e da o resultado do tempo corrido
                if tempo_corrido < timedelta(seconds=3):          # Se o tempo corrido for menor que 1 Hora, ele vai entra no bloco e esperar dar 1 Hora
                    tempo_esperando = (timedelta(seconds=3) - tempo_corrido).total_seconds()
                    log_and_print(f"Esperando {tempo_esperando} segundos para não sobrecarregar a API.")
                    time.sleep(tempo_esperando)
                tempo_inicio = datetime.now()
                df.to_csv('resultado_geolocalizacao.CSV', index=False)
                log_and_print(f"Resultados parciais salvos no arquivo 'resultado_geolocalizacao.csv'")
        except Exception as e:
            log_and_print(f"Erro durante o processamento do CEP {row['CEP']}: {e}")

df.to_csv('resultado_geolocalizacao.csv', index=False) # Cria um arquivo CSV com os dados atualizados
log_and_print("Geolocalizações Adicionadas com sucesso ao DataFrame.")