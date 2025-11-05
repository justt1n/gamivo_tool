"""
The core business logic orchestrator for the price update process.
VERSION: ASYNCHRONOUS + CHUNKING + JITTER
"""
import logging
import random
import asyncio
from datetime import datetime
from time import monotonic
from typing import List, Tuple, Optional, Dict, Any

# IMPORTANT: Make sure to import the updated GamivoClient
from clients.gamivo_client import GamivoClient, GamivoAPIError
from clients.google_sheets_client import GoogleSheetsClient
from models.sheet_models import Payload
# Assuming these components are in their respective paths
from utils.config import Config


# --- HÀM HELPER MỚI CHO CHUNKING ---
def _create_chunks(data: List[Any], chunk_size: int) -> List[List[Any]]:
    """Chia danh sách data thành các lô (chunks) có kích thước chunk_size."""
    if chunk_size <= 0:
        chunk_size = 1
    return [data[i:i + chunk_size] for i in range(0, len(data), chunk_size)]


# --- KẾT THÚC HÀM HELPER ---


class PriceProcessor:
    """
    Orchestrates the entire process of fetching data, processing prices,
    and logging results using asyncio.
    """

    def __init__(self, config: Config, gamivo_client: GamivoClient):
        self.config = config
        self.gsheet_client = GoogleSheetsClient(config.google_key_path)
        self.gamivo_client = gamivo_client  # Nhận client đã được khởi tạo
        self.blacklist: List[str] = []
        self.log_buffer: List[dict] = []
        self._log_lock = asyncio.Lock()  # Lock để bảo vệ log_buffer

    async def _fetch_blacklist(self, payload: Payload):
        """Fetches the blacklist from the sheet (Async)."""
        loc = payload.blacklist_location
        if not all([loc.sheet_id, loc.sheet_name, loc.cell]):
            logging.warning("Blacklist location is not fully configured. Skipping fetch.")
            self.blacklist = []
            return

        range_name = f"{loc.sheet_name}!{loc.cell}"
        # Chạy tác vụ blocking trong một thread riêng
        blacklist_data = await asyncio.to_thread(
            self.gsheet_client.get_data,
            loc.sheet_id,
            range_name
        )
        self.blacklist = [item for sublist in blacklist_data for item in sublist if item]
        logging.info(f"Fetched {len(self.blacklist)} sellers for the blacklist.")

    async def _get_price_boundaries(self, payload: Payload) -> Tuple[float, float, int]:
        """Fetches price boundaries from Google Sheets (Async)."""
        min_loc, max_loc, stock_loc = payload.min_price_location, payload.max_price_location, payload.stock_location

        # Chạy các tác vụ blocking trong thread riêng
        min_price_raw = (await asyncio.to_thread(
            self.gsheet_client.get_data, min_loc.sheet_id, f"{min_loc.sheet_name}!{min_loc.cell}"
        ))[0][0]
        max_price_raw = (await asyncio.to_thread(
            self.gsheet_client.get_data, max_loc.sheet_id, f"{max_loc.sheet_name}!{max_loc.cell}"
        ))[0][0]
        stock_raw = (await asyncio.to_thread(
            self.gsheet_client.get_data, stock_loc.sheet_id, f"{stock_loc.sheet_name}!{stock_loc.cell}"
        ))[0][0]

        return float(min_price_raw), float(max_price_raw), int(stock_raw)

    async def _analyze_offers(self, product_id: int, my_seller_name: Optional[str]) -> Dict[str, any]:
        """
        Analyzes product offers to find the valid competitor (Async).
        """
        # Client call bây giờ là async
        offers = await self.gamivo_client.get_product_offers(product_id)
        sorted_offers = sorted(offers, key=lambda o: o.retail_price)
        analysis = {"valid_competitor": None, "top_sellers_for_log": sorted_offers[:4]}

        for offer in sorted_offers:
            if offer.seller_name != my_seller_name and offer.seller_name not in self.blacklist:
                analysis["valid_competitor"] = offer
                break
        return analysis

    def _calculate_final_price(self, target_price: float, min_change: float, max_change: float, rounding: int,
                               min_price: float,
                               max_price: float) -> float:
        """
        Calculates the final price (No I/O, không cần async).
        """
        random_price = random.uniform(min_change, max_change)
        ideal_price = target_price - random_price
        price_after_min_check = max(min_price, ideal_price)
        final_price = min(max_price, price_after_min_check)
        return round(final_price, rounding)

    async def _process_single_payload(self, payload: Payload):
        """Processes a single payload asynchronously."""

        # --- THÊM JITTER ---
        # Thêm một khoảng nghỉ ngẫu nhiên nhỏ (0.1s - 1.5s)
        # để tránh 5 worker cùng lúc đập vào server và bị firewall chặn (gây lỗi SSL).
        jitter = random.uniform(0.1, 1.5)
        await asyncio.sleep(jitter)
        # --- KẾT THÚC JITTER ---

        try:
            # --- PHASE 1: SETUP & CALCULATE (Luôn tính toán) ---
            min_price, max_price, stock = await self._get_price_boundaries(payload)
            offer_id = await self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
            if not offer_id:
                raise Exception(f"Offer ID not found for product {payload.product_compare_id}")

            my_offer_data = await self.gamivo_client.retrieve_my_offer(offer_id)
            my_seller_name = my_offer_data.get('seller_name')
            my_current_price = my_offer_data.get('seller_price', float('inf'))

            offer_analysis = await self._analyze_offers(payload.product_compare_id, my_seller_name)
            valid_competitor = offer_analysis["valid_competitor"]

            final_price = max_price
            competitor_price = 0.0
            competitor_seller = "Không đối thủ"

            if valid_competitor:
                competitor_seller = valid_competitor.seller_name
                competitor_price_raw = valid_competitor.retail_price
                competitor_price = (await self.gamivo_client.calculate_seller_price(offer_id,
                                                                                    competitor_price_raw)).seller_price

                final_price = self._calculate_final_price(
                    competitor_price,
                    payload.min_change_price,
                    payload.max_change_price,
                    payload.rounding_precision,
                    min_price,
                    max_price
                )

            # --- PHASE 2: DECIDE (Quyết định có update hay không) ---
            should_update = False
            log_msg_if_skipped = ""

            if payload.get_mode == 1:
                should_update = True
            else:
                if my_current_price > final_price:
                    should_update = True
                else:
                    log_lines = [
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Không cập nhật",
                        f"Giá hiện tại: {my_current_price:.2f} | Giá mục tiêu: {final_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}"
                    ]
                    log_lines.extend(await self._get_top_sellers_log(payload.product_compare_id, offer_id))  # await
                    log_msg_if_skipped = "\n".join(log_lines)

            # --- PHASE 3: ACTION (Chỉ 1 nơi gọi API, 1 nơi xử lý log) ---
            if should_update:
                status, response = await self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)

                if status == 200:
                    log_lines = [
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá đã cập nhật thành công; Price = {final_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}"
                    ]
                    log_lines.extend(await self._get_top_sellers_log(payload.product_compare_id, offer_id))  # await
                    log_msg = "\n".join(log_lines)

                    logging.info(f"Successfully processed row {payload.sheet_row_num}. Log details:\n{log_msg}")
                    await self._add_log(payload.sheet_row_num, log_msg, 'C')  # await
                else:
                    log_msg = f"Failed to process: {response}"
                    await self._add_log(payload.sheet_row_num, log_msg, 'E')  # await
                    raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
            else:
                logging.info(
                    f"Skipping update for row {payload.sheet_row_num} (Mode 2). Log details:\n{log_msg_if_skipped}")
                await self._add_log(payload.sheet_row_num, log_msg_if_skipped, 'C')  # await

        except (GamivoAPIError, Exception) as e:
            # Đây là lỗi CUỐI CÙNG (sau khi retry đã thất bại)
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            await self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')  # await
        finally:
            await self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')  # await

    async def _get_top_sellers_log(self, product_id: int, offer_id: int) -> List[str]:
        """
        Helper method to fetch and format the top 4 sellers (Async).
        """
        log_lines = ["----Top Sellers----"]
        seller_prefixes = ["1st", "2nd", "3rd", "4th"]
        try:
            log_offers = await self.gamivo_client.get_product_offers(product_id)
            log_sorted_offers = sorted(log_offers, key=lambda o: o.retail_price)[:4]

            price_tasks = []
            for offer in log_sorted_offers:
                price_tasks.append(self.gamivo_client.calculate_seller_price(offer_id, offer.retail_price))

            calculated_prices = await asyncio.gather(*price_tasks)

            for i, (offer, calculated) in enumerate(zip(log_sorted_offers, calculated_prices)):
                log_lines.append(
                    f" - {seller_prefixes[i]} Seller: {offer.seller_name} - Price: {offer.retail_price} ({calculated.seller_price:.2f})")
        except Exception as e:
            logging.warning(f"Could not generate top sellers log for product {product_id}: {e}")
            log_lines.append(" - Error fetching top sellers.")
        return log_lines

    async def _add_log(self, row_num: int, message: str, column: str):
        """Adds a log message to the buffer safely (Async)."""
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        # Khóa buffer để tránh race condition
        async with self._log_lock:
            self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    async def _flush_logs(self):
        """Flushes the log buffer to Google Sheets (Async)."""
        if not self.log_buffer:
            return

        logs_to_flush = []
        # Khóa buffer để lấy log và xóa
        async with self._log_lock:
            if not self.log_buffer:
                return
            logs_to_flush = self.log_buffer.copy()
            self.log_buffer.clear()

        if not logs_to_flush:
            return

        try:
            # Chạy tác vụ GSheet blocking trong thread riêng
            await asyncio.to_thread(
                self.gsheet_client.batch_update,
                self.config.main_sheet_id,
                logs_to_flush
            )
            logging.info(f"Wrote {len(logs_to_flush)} log entries to the sheet.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")

    async def run(self):
        """The main processing loop (Async + Chunking)."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            try:
                # --- SỬA: Bỏ `create_connection` ---
                # (SQLiteClient mới sẽ tự quản lý)
                # await asyncio.to_thread(self.gamivo_client.db_client.create_connection)

                range_to_fetch = f"{self.config.main_sheet_name}!A{self.config.start_row}:AC"
                # Chạy GSheet get_data trong thread
                sheet_data = await asyncio.to_thread(
                    self.gsheet_client.get_data,
                    self.config.main_sheet_id,
                    range_to_fetch
                )

                payloads = [Payload.from_row(row_data, self.config.start_row + i) for i, row_data in
                            enumerate(sheet_data)]
                valid_payloads = [p for p in payloads if p and p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                else:
                    await self._fetch_blacklist(valid_payloads[0])  # await

                logging.info(f"Found {len(valid_payloads)} enabled products to process.")

                num_workers = self.config.max_workers
                logging.info(f"Using a pool of {num_workers} concurrent workers.")

                payload_chunks = _create_chunks(valid_payloads, num_workers)

                logging.info(f"Processing {len(valid_payloads)} payloads in {len(payload_chunks)} chunks.")

                for i, chunk in enumerate(payload_chunks):
                    start_chunk_time = monotonic()
                    logging.info(f"--- Processing Chunk {i + 1}/{len(payload_chunks)} ({len(chunk)} items) ---")

                    tasks = []
                    for payload in chunk:
                        logging.info(f"Queueing '{payload.product_name}' from row {payload.sheet_row_num}...")
                        tasks.append(self._process_single_payload(payload))

                    await asyncio.gather(*tasks, return_exceptions=True)

                    await self._flush_logs()

                    end_chunk_time = monotonic()
                    logging.info(f"--- Finished Chunk {i + 1} in {end_chunk_time - start_chunk_time:.2f} seconds ---")

                    if i < len(payload_chunks) - 1:  # Không ngủ sau lô cuối
                        logging.info(f"Cooling down for {self.config.retries_time_sleep}s before next chunk...")
                        await asyncio.sleep(self.config.retries_time_sleep)

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
                await self._flush_logs()  # Cố gắng flush log nếu có lỗi

            finally:
                # --- SỬA: Bỏ `close_connection` ---
                # (SQLiteClient mới sẽ tự quản lý)
                pass

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            await asyncio.sleep(self.config.loop_delay_seconds)
