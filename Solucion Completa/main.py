from src.ingestion.ingest import ingest_data
from src.cleaning.clean import clean_data
from src.transformation.transform import transform_data
from src.utils.logger import setup_logger


def run_pipeline():
    setup_logger()

    df = ingest_data()
    df_clean = clean_data(df)
    transform_data(df_clean)


if __name__ == "__main__":
    run_pipeline()