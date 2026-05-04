import pandas as pd
import logging
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "transformed", "clientes_transformados.csv")


def transform_data(df):
    logging.info("==== INICIO TRANSFORMACIÓN ====")

    df_trans = df.copy()

    df_trans["edad"] = pd.to_numeric(df_trans["edad"], errors="coerce")

    bins = [0, 18, 30, 50, 100]
    labels = ["Menor", "Joven", "Adulto", "Adulto Mayor"]
    df_trans["grupo_edad"] = pd.cut(df_trans["edad"], bins=bins, labels=labels)

    df_trans["ingreso_normalizado"] = (
        df_trans["ingreso_mensual"] - df_trans["ingreso_mensual"].min()
    ) / (
        df_trans["ingreso_mensual"].max() - df_trans["ingreso_mensual"].min()
    )

    df_trans["ciudad_cod"] = df_trans["ciudad"].astype("category").cat.codes

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    df_trans.to_csv(OUTPUT_PATH, index=False)

    logging.info("==== FIN TRANSFORMACIÓN ====")

    return df_trans