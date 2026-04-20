import requests
import logging
import pandas as pd
import os
import time

# Crear carpetas si no existen
os.makedirs("data/raw", exist_ok=True)
os.makedirs("logs", exist_ok=True)

# Configuración de logs
logging.basicConfig(
    filename='logs/ingesta.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

BASE_URL = "https://rickandmortyapi.com/api/character"
DESTINO = "data/raw/personajes.csv"

def obtener_todos_los_personajes():
    personajes = []
    url = BASE_URL

    while url:
        response = requests.get(url)

        # Manejo de rate limit
        if response.status_code == 429:
            print("⏳ Demasiadas solicitudes, esperando...")
            logging.warning("Rate limit alcanzado (429), esperando...")
            time.sleep(2)
            continue

        if response.status_code != 200:
            raise Exception(f"Error en API: {response.status_code}")

        data = response.json()

        # Agregar resultados
        personajes.extend(data["results"])

        # Ir a la siguiente página
        url = data["info"]["next"]

        # Pausa para evitar saturar la API
        time.sleep(0.5)

    return personajes

def ejecutar_ingesta():
    logging.info("Inicio de ingesta PRO (API + incremental)")

    try:
        # 1. Obtener datos desde API
        personajes = obtener_todos_los_personajes()
        df_api = pd.DataFrame(personajes)

        # 2. Limpieza de datos (columnas anidadas)
        df_api["origen"] = df_api["origin"].apply(lambda x: x["name"])
        df_api["ubicacion"] = df_api["location"].apply(lambda x: x["name"])

        df_api = df_api.drop(columns=["origin", "location", "episode"])

        # 3. Primera carga (si no existe archivo)
        if not os.path.exists(DESTINO):
            df_api.to_csv(DESTINO, index=False)
            logging.info(f"Archivo creado. Registros: {len(df_api)}")
            print(f"✅ Primera ingesta: {len(df_api)} registros")
            return

        # 4. Leer archivo existente
        df_actual = pd.read_csv(DESTINO)

        # 5. Identificar nuevos registros
        nuevos = df_api[~df_api["id"].isin(df_actual["id"])]

        if nuevos.empty:
            logging.info("No hay nuevos personajes para agregar")
            print("ℹ️ No hay nuevos registros")
        else:
            # 6. Agregar nuevos registros
            df_actualizado = pd.concat([df_actual, nuevos])
            df_actualizado.to_csv(DESTINO, index=False)

            logging.info(f"Nuevos registros agregados: {len(nuevos)}")
            print(f"✅ Se agregaron {len(nuevos)} personajes nuevos")

    except Exception as e:
        logging.error(f"Error en la ingesta: {e}")
        print("❌ Error en la ingesta, revisar logs/ingesta.log")

if __name__ == "__main__":
    ejecutar_ingesta()