# main.py
import logging
import sys
import asyncio  # Import asyncio

from logic.price_processor import PriceProcessor
from utils.config import Config


async def main():
    """
    Initializes the application and starts the main processing service.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    logging.info("Application starting up...")

    processor = None
    try:
        config = Config()
        processor = PriceProcessor(config)

        # Chạy vòng lặp bất đồng bộ
        await processor.run()

    except Exception as e:
        logging.critical(f"Failed to initialize or run the application: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if processor and processor.gamivo_client:
            logging.info("Shutting down HTTP client...")
            await processor.gamivo_client.__aexit__(None, None, None)
            logging.info("HTTP client shut down.")


if __name__ == "__main__":
    # Sử dụng asyncio.run() để khởi chạy hàm main bất đồng bộ
    asyncio.run(main())