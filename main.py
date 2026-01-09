from src.config_loader import load_config
from src.logging_config import setup_logging
from src.data_ingestor import CricketDataIngestor
from pathlib import Path

def main():
    cfg = load_config()
    logger = setup_logging(cfg) 
    paths = cfg['paths']
    logger.info("HowzatForData Started")

    # Ingest Data
    ingestor = CricketDataIngestor(paths['raw_data'])
    df = ingestor.ingest_all()
    logger.info(f"Ingested {len(df)} rows")

    # Save Ingested Data
    ingested_dir = Path(paths['ingested_data'])
    ingested_path = ingested_dir / "cricket_data.parquet"
    ingested_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(ingested_path, index=False)
    logger.info(f"Ingested Data saved to parquet at {ingested_path}")

    logger.info("HowzatForData Completed")

if __name__ == "__main__":
    main()