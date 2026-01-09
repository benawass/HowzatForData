#!/usr/bin/env python3
"""
HowzatForData - Cricket Data Pipeline CLI

A command-line tool for ingesting and preprocessing cricket match data.
"""
import typer
from typing_extensions import Annotated

from src.config_loader import load_config
from src.logging_config import setup_logging
from src.data_ingestor import CricketDataIngestor
from src.data_preprocessor import CricketDataPreprocessor
from src.utils import save_to_parquet, load_from_parquet

app = typer.Typer(
    name="howzat",
    help="""HowzatForData: Process cricket match data from raw JSON files into analysis-ready
    parquet format with validation and preprocessing.
    """,
    add_completion=False,
    rich_markup_mode="rich",
)


def get_logger():
    """Initialize config and logging, return logger and paths."""
    cfg = load_config()
    logger = setup_logging(cfg)
    return cfg, logger


@app.command()
def ingest():
    """
    [bold green]Ingest[/] raw JSON cricket data into parquet format.
    
    Parses ball-by-ball JSON files from the raw data directory and saves
    them as a single parquet file for efficient processing.
    """
    cfg, logger = get_logger()
    paths = cfg["paths"]
    
    logger.info("Starting data ingestion...")
    ingestor = CricketDataIngestor(paths["raw_data"])
    df = ingestor.ingest_all()
    
    save_to_parquet(df, paths["ingested_data"], "Ingested data")
    logger.info("Ingestion complete!")


@app.command()
def preprocess(
    year: Annotated[
        int,
        typer.Option(
            "--year", "-y",
            help="Filter to matches from this year onwards",
        ),
    ] = 2015,
):
    """
    [bold blue]Preprocess[/] and validate ingested data.
    
    Clean data, filter by date, and validate match outcomes.
    Outputs a preprocessed parquet file ready for ML pipelines.
    """
    cfg, logger = get_logger()
    paths = cfg["paths"]
    
    logger.info("Starting preprocessing...")
    df = load_from_parquet(paths["ingested_data"], "Ingested data")
    
    preprocessor = CricketDataPreprocessor(df, modern_data_filter_year=year)
    df_clean = preprocessor.clean_data()
    
    save_to_parquet(df_clean, paths["preprocessed_data"], "Preprocessed data")
    logger.info("Preprocessing complete!")


@app.command(name="all")
def run_all(
    year: Annotated[
        int,
        typer.Option(
            "--year", "-y",
            help="Filter to matches from this year onwards",
        ),
    ] = 2015,
):
    """
    [bold magenta]Run the full pipeline[/]: ingest + preprocess.
    
    Convenience command to run ingestion and preprocessing in sequence.
    """
    ingest()
    preprocess(year=year)


if __name__ == "__main__":
    app()