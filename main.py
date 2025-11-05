import logging
import sys
import asyncio
import httpx

from clients.gamivo_client import GamivoClient
from logic.price_processor import PriceProcessor
from utils.config import Config


async def main():
    """
    Initializes the application and starts the main processing service.
    """
    # Cấu hình logging
    logging.basicConfig(
        level=logging.INFO,  # Đặt là INFO
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )
    # Tắt bớt log của thư viện
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    logging.info("Application starting up...")

    try:
        config = Config()

        # Tạo httpx.AsyncClient làm context manager
        # --- THAY ĐỔI: Giảm timeout xuống 15 giây ---
        async with httpx.AsyncClient(timeout=15.0) as http_client:

            # Khởi tạo GamivoClient và truyền http_client VÀ config retry vào
            gamivo_client = GamivoClient(
                api_key=config.gamivo_api_key,
                db_path=config.db_path,
                client=http_client,
                retries=config.retries_time,  # <-- Dùng config
                retry_sleep_max=config.retries_time_sleep  # <-- Dùng config
            )

            # Khởi tạo PriceProcessor và truyền gamivo_client vào
            processor = PriceProcessor(config, gamivo_client)

            # Chạy vòng lặp processor (giờ là async)
            await processor.run()

    except Exception as e:
        logging.critical(f"Failed to initialize or run the application: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Dùng asyncio.run() để khởi chạy hàm main async
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Application shut down by user.")