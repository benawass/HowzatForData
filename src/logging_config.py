import logging
import os
from rich.logging import RichHandler

def setup_logging(cfg):
    log_file = cfg['paths']['log_file']
    os.makedirs(os.path.dirname(log_file), exist_ok=True)

    console_handler = RichHandler(rich_tracebacks=True, show_path=True)
    
    file_handler = logging.FileHandler(log_file)
    file_handler.setFormatter(logging.Formatter(
        '%(asctime)s | %(levelname)-8s | %(name)s:%(lineno)d | %(message)s'
    ))

    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[console_handler, file_handler],
        force=True
    )
    return logging.getLogger("HowzatForData") # Returns the base logger