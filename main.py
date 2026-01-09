#!/usr/bin/env python3
"""
HowzatForData - Cricket Data Pipeline CLI

A command-line tool for ingesting and preprocessing cricket match data.
"""
import argparse
import sys

from src.config_loader import load_config
from src.logging_config import setup_logging
from src.data_ingestor import CricketDataIngestor
from src.data_preprocessor import CricketDataPreprocessor
from src.utils import save_to_parquet, load_from_parquet


def cmd_ingest(args, cfg, logger):
    """Ingest raw JSON cricket data into parquet format."""
    paths = cfg["paths"]
    
    logger.info("Starting data ingestion...")
    ingestor = CricketDataIngestor(paths["raw_data"])
    df = ingestor.ingest_all()
    
    save_to_parquet(df, paths["ingested_data"], "Ingested data")
    logger.info("Ingestion complete!")


def cmd_preprocess(args, cfg, logger):
    """Preprocess ingested data for ML pipelines."""
    paths = cfg["paths"]
    
    logger.info("Starting preprocessing...")
    df = load_from_parquet(paths["ingested_data"], "Ingested data")
    
    preprocessor = CricketDataPreprocessor(df, modern_data_filter_year=args.year)
    df_clean = preprocessor.clean_data()
    
    save_to_parquet(df_clean, paths["preprocessed_data"], "Preprocessed data")
    logger.info("Preprocessing complete!")


def cmd_run_all(args, cfg, logger):
    """Run the full pipeline: ingest + preprocess."""
    cmd_ingest(args, cfg, logger)
    cmd_preprocess(args, cfg, logger)


def create_parser():
    """Create the CLI argument parser with pretty help."""
    parser = argparse.ArgumentParser(
        prog="howzat",
        description="""
╔═══════════════════════════════════════════════════════════════╗
║                      HowzatForData                            ║
║         Cricket Ball-by-Ball Data Processing Pipeline         ║
╚═══════════════════════════════════════════════════════════════╝

Process cricket match data from raw JSON files into analysis-ready
parquet format with validation and preprocessing.
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python main.py ingest        Ingest raw JSON files to parquet
  python main.py preprocess    Clean and validate ingested data
  python main.py all           Run full pipeline (ingest + preprocess)
  python main.py preprocess --year 2020   Filter to matches from 2020+
        """,
    )

    subparsers = parser.add_subparsers(
        title="Commands",
        dest="command",
        metavar="<command>",
    )

    # Ingest command
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Ingest raw JSON cricket data into parquet format",
        description="Parse ball-by-ball JSON files and save to parquet.",
    )
    ingest_parser.set_defaults(func=cmd_ingest)

    # Preprocess command
    preprocess_parser = subparsers.add_parser(
        "preprocess",
        help="Preprocess and validate ingested data",
        description="Clean data, filter by date, and validate match outcomes.",
    )
    preprocess_parser.add_argument(
        "--year",
        type=int,
        default=2015,
        metavar="YEAR",
        help="Filter to matches from this year onwards (default: 2015)",
    )
    preprocess_parser.set_defaults(func=cmd_preprocess)

    # All command (run full pipeline)
    all_parser = subparsers.add_parser(
        "all",
        help="Run the full pipeline (ingest + preprocess)",
        description="Run ingestion and preprocessing in sequence.",
    )
    all_parser.add_argument(
        "--year",
        type=int,
        default=2015,
        metavar="YEAR",
        help="Filter to matches from this year onwards (default: 2015)",
    )
    all_parser.set_defaults(func=cmd_run_all)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    # Initialize config and logging
    cfg = load_config()
    logger = setup_logging(cfg)
    
    logger.info(f"HowzatForData - Running '{args.command}' command")
    
    try:
        args.func(args, cfg, logger)
    except FileNotFoundError as e:
        logger.error(str(e))
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Command failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()