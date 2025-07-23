import logging
import sys

from logic.price_processor import PriceProcessor
from utils.config import Config


def main():
    """
    Initializes the application and starts the main processing service.
    """
    # Configure logging to show timestamp, level, and message
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout  # Log to standard output
    )

    logging.info("Application starting up...")

    try:
        # Load configuration from .env file
        config = Config()

        # Initialize the main processor with the loaded configuration
        processor = PriceProcessor(config)

        # Start the infinite processing loop
        processor.run()

    except Exception as e:
        logging.critical(f"Failed to initialize or run the application: {e}", exc_info=True)
        # In a real-world scenario, you might want to send an alert here.
        sys.exit(1)  # Exit with an error code


if __name__ == "__main__":
    main()
