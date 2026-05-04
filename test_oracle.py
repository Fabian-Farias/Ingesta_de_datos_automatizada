import oracledb
import pandas as pd

def conectar():
    connection = oracledb.connect(
            user="ADMIN",

            # 🔴 TU PASSWORD
            password="Pax.,ytrG231",

            # 🔴 NOMBRE DEL SERVICIO (revisar archivo tnsnames.ora en la wallet)
            # ejemplos: "mydb_high", "mydb_medium", "mydb_low"
            dsn="desarrollopipeline_high",

            # 🔴 RUTA DONDE DESCOMPRIMISTE LA WALLET
            config_dir=r"C:/Users/SSDD/Desktop/Ingesta_de_datos_automatizada/Wallet_DesarrolloPipeline",

            # 🔴 MISMA RUTA DE LA WALLET
            wallet_location=r"C:/Users/SSDD/Desktop/Ingesta_de_datos_automatizada/Wallet_DesarrolloPipeline",

    )
    return connection


def test_query():
    conn = conectar()

    print("Conectado a Oracle")

    # 🔥 prueba 1
    df_count = pd.read_sql("SELECT COUNT(*) as total FROM clientes", conn)
    print("\nCantidad de registros:")
    print(df_count)

    # 🔥 prueba 2
    df_data = pd.read_sql("SELECT * FROM clientes FETCH FIRST 5 ROWS ONLY", conn)
    print("\nPrimeros registros:")
    print(df_data)

    conn.close()


if __name__ == "__main__":
    test_query()