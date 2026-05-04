import pandas as pd
import logging
import os
import oracledb

# 🔷 BASE DEL PROYECTO
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HISTORICO_PATH = os.path.join(BASE_DIR, "data", "raw", "clientes_historico.csv")



# 🔷 FUNCIÓN: Conexión a Oracle Cloud usando Wallet
def conectar_oracle():
    try:
        connection = oracledb.connect(

            # 🔴 TU USUARIO DE ORACLE
            user="ADMIN",

            # 🔴 TU PASSWORD
            password="Pax.,ytrG231",

            # 🔴 NOMBRE DEL SERVICIO (revisar archivo tnsnames.ora en la wallet)
            # ejemplos: "mydb_high", "mydb_medium", "mydb_low"
            dsn="desarrollopipeline_high",

            # 🔴 RUTA DONDE DESCOMPRIMISTE LA WALLET
            config_dir=r"C:/Users/SSDD/Desktop/Gestion_de_Datos_IA/Ingesta_de_datos_automatizada/Wallet_DesarrolloPipeline",

            # 🔴 MISMA RUTA DE LA WALLET
            wallet_location=r"C:/Users/SSDD/Desktop/Gestion_de_Datos_IA/Ingesta_de_datos_automatizada/Wallet_DesarrolloPipeline",

            # 🔴 OPCIONAL (solo si tu wallet tiene contraseña)
            # wallet_password="TU_WALLET_PASSWORD"
        )


        logging.info("Conexión a Oracle exitosa")
        return connection

    except Exception as e:
        logging.error(f"Error conexión Oracle: {e}")
        raise


def obtener_datos_db():
    conn = conectar_oracle()
    query = "SELECT * FROM clientes"
    df = pd.read_sql(query, conn)

# 🔥 NORMALIZAR NOMBRES DE COLUMNAS
    df.columns = df.columns.str.lower()
    conn.close()

    logging.info(f"Registros obtenidos: {len(df)}")
    return df


def ingest_data():
    logging.info("==== INICIO INGESTA ====")

    df_nuevo = obtener_datos_db()

    # 🔥 crear carpeta automáticamente
    os.makedirs(os.path.dirname(HISTORICO_PATH), exist_ok=True)

    if os.path.exists(HISTORICO_PATH):
        df_actual = pd.read_csv(HISTORICO_PATH)

        df_merge = df_nuevo.merge(df_actual, on="id", how="left", indicator=True)
        df_nuevos = df_merge[df_merge["_merge"] == "left_only"]
        df_nuevos = df_nuevos[df_nuevo.columns]

        if df_nuevos.empty:
            logging.info("No hay nuevos registros")
            return df_actual

        df_actualizado = pd.concat([df_actual, df_nuevos], ignore_index=True)

    else:
        logging.info("Creando histórico inicial")
        df_actualizado = df_nuevo

    df_actualizado.to_csv(HISTORICO_PATH, index=False)

    logging.info(f"Total histórico: {len(df_actualizado)}")
    logging.info("==== FIN INGESTA ====")

    return df_actualizado