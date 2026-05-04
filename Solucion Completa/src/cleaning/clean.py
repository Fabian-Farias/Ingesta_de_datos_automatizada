import pandas as pd
import logging
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
OUTPUT_PATH = os.path.join(BASE_DIR, "data", "processed", "clientes_limpios.csv")


def clean_data(df):
    logging.info("==== INICIO LIMPIEZA ====")

    df_clean = df.copy()

    logging.info(f"Registros iniciales: {len(df_clean)}")

    # 🔷 1. ELIMINAR SOLO LO CRÍTICO
    df_clean = df_clean.dropna(subset=["id", "nombre"])
    logging.info(f"Después de eliminar sin ID o nombre: {len(df_clean)}")

    # 🔷 2. NORMALIZAR TEXTO
    df_clean["nombre"] = df_clean["nombre"].astype(str).str.title().str.strip()
    df_clean["ciudad"] = df_clean["ciudad"].astype(str).str.lower().str.strip()

    # 🔷 3. LIMPIEZA DE CORREO
    df_clean["correo"] = df_clean["correo"].astype(str).str.lower().str.strip()
    df_clean["correo"] = df_clean["correo"].replace(["none", "nan", ""], None)

    # eliminar correos inválidos
    df_clean = df_clean[df_clean["correo"].str.contains("@", na=False)]

    # 🔷 4. LIMPIEZA DE FECHAS (🔥 OPCIÓN 1)
    df_clean["fecha_registro"] = pd.to_datetime(
        df_clean["fecha_registro"],
        errors="coerce"
    )

    # eliminar registros sin fecha válida
    df_clean = df_clean.dropna(subset=["fecha_registro"])

    logging.info(f"Después de procesar fechas: {len(df_clean)}")

    # 🔷 5. LIMPIEZA DE INGRESOS
    df_clean["ingreso_mensual"] = pd.to_numeric(
        df_clean["ingreso_mensual"],
        errors="coerce"
    )

    promedio_ingreso = df_clean["ingreso_mensual"].mean()
    df_clean["ingreso_mensual"] = df_clean["ingreso_mensual"].fillna(promedio_ingreso)

    logging.info(f"Después de procesar ingresos: {len(df_clean)}")

    # 🔷 6. EDAD
    df_clean["edad"] = pd.to_numeric(df_clean["edad"], errors="coerce")
    df_clean["edad"] = df_clean["edad"].fillna(df_clean["edad"].median())
    df_clean["edad"] = df_clean["edad"].astype(int)

    # 🔷 7. ELIMINAR DUPLICADOS
    df_clean = df_clean.sort_values("fecha_registro")
    df_clean = df_clean.drop_duplicates(subset=["id"], keep="last")
    df_clean = df_clean.drop_duplicates(subset=["nombre", "correo"])

    logging.info(f"Después de eliminar duplicados: {len(df_clean)}")

    # 🔷 8. CREAR CARPETA SI NO EXISTE
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

    df_clean.to_csv(OUTPUT_PATH, index=False)

    logging.info("Archivo clientes_limpios.csv generado")
    logging.info("==== FIN LIMPIEZA ====")

    return df_clean