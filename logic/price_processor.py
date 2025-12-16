"""
The core business logic orchestrator for the price update process.
ASYNCHRONOUS version with concurrency limiting and MULTI-FILE BATCH READ.
"""
import asyncio
import logging
import random
import re  # <<< IMPORT ĐÃ ĐƯỢC THÊM
from collections import defaultdict
from datetime import datetime
from time import monotonic
from typing import List, Tuple, Optional, Dict

from clients.gamivo_client import GamivoClient, GamivoAPIError
from clients.google_sheets_client import GoogleSheetsClient
from models.gamivo_models import OfferDetails
from models.sheet_models import Payload  # Đảm bảo bạn import Payload từ đúng file
from utils.config import Config


class PriceProcessor:
    """
    Orchestrates the entire process of fetching data, processing prices,
    and logging results.
    """

    def __init__(self, config: Config):
        self.config = config
        self.gsheet_client = GoogleSheetsClient(config.google_key_path)
        self.gamivo_client = GamivoClient(
            api_key=config.gamivo_api_key,
            db_path=config.db_path
        )
        self.blacklist: List[str] = []
        self.log_buffer: List[dict] = []
        self.gsheet_lock = asyncio.Lock()  # Lock để bảo vệ các lệnh gọi Google API

    # --- HÀM HELPER (ĐÃ SỬA) ---

    def _get_price_boundaries(self, payload: Payload, config_cache: dict) -> Tuple[float, float, int]:
        """
        (ĐỒNG BỘ) Lấy 3 giá trị từ cache đã được đọc trước.
        (ĐÃ SỬA) Sử dụng key chuẩn hóa (không có dấu ').
        """
        min_loc = payload.min_price_location
        max_loc = payload.max_price_location
        stock_loc = payload.stock_location

        # Build các key chuẩn hóa (KHÔNG CÓ DẤU ' quanh tên sheet)
        # Sẽ khớp với key được tạo trong hàm run()
        min_key = f"{min_loc.sheet_id}:{min_loc.sheet_name}!{min_loc.cell}"
        max_key = f"{max_loc.sheet_id}:{max_loc.sheet_name}!{max_loc.cell}"
        stock_key = f"{stock_loc.sheet_id}:{stock_loc.sheet_name}!{stock_loc.cell}"

        try:
            # Lấy giá trị từ cache.
            min_price_raw = config_cache[min_key][0][0]
            max_price_raw = config_cache[max_key][0][0]
            stock_raw = config_cache[stock_key][0][0]

            return float(min_price_raw), float(max_price_raw), int(stock_raw)

        except KeyError as e:
            # Log lỗi rõ ràng để biết key nào bị thiếu
            logging.error(f"Key {e} not found in config_cache for payload {payload.product_name}.")
            logging.error(f"Lý do: Ô này có thể bị rỗng trên Google Sheet hoặc cấu hình sai.")
            raise Exception(f"Missing config value for {payload.product_name} (key: {e})")
        except (ValueError, IndexError, TypeError) as e:
            logging.error(f"Failed to parse config values for {payload.product_name} (keys: {min_key}, {max_key}, {stock_key}): {e}")
            raise Exception(f"Invalid config format for {payload.product_name}")

    # --- HÀM HELPER MỚI ---
    async def _execute_batch_get(self, sheet_id: str, ranges: List[str]) -> Tuple[str, List[dict]]:
        """
        Thực thi một lệnh batch_get cho một sheet_id cụ thể và trả về kết quả.
        """
        if not sheet_id or not ranges:
            return (sheet_id, [])

        try:
            logging.info(f"Executing batch_get for {sheet_id} with {len(ranges)} ranges.")
            async with self.gsheet_lock: # Bảo vệ Google client
                value_ranges = await asyncio.to_thread(
                    self.gsheet_client.batch_get,
                    sheet_id,
                    ranges
                )
            return (sheet_id, value_ranges)
        except Exception as e:
            logging.error(f"Failed batch_get for sheet_id {sheet_id}: {e}")
            return (sheet_id, []) # Trả về rỗng để gather không bị vỡ

    # --- CÁC HÀM LOGIC CỐT LÕI (Không thay đổi) ---

    async def _analyze_offers(self, product_id: int, my_seller_name: Optional[str]) -> Dict[str, any]:
        """
        Analyzes product offers to find the valid competitor and top sellers for logging.
        """
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
        random_price = random.uniform(min_change, max_change)
        ideal_price = target_price - random_price
        price_after_min_check = max(min_price, ideal_price)
        final_price = min(max_price, price_after_min_check)
        return round(final_price, rounding)

    # --- HÀM XỬ LÝ PAYLOAD (Không thay đổi) ---

    async def _process_single_payload(self, payload: Payload, config_cache: dict):
        """
        Xử lý logic cho một payload duy nhất.
        Nhận config_cache thay vì tự gọi API.
        """
        precalculated_prices: Dict[str, float] = {}
        log_lines_sellers = []
        try:
            # --- PHASE 1: SETUP & CALCULATE ---

            # Đọc từ cache (Đồng bộ)
            min_price, max_price, stock = self._get_price_boundaries(payload, config_cache)

            offer_id = self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
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

                competitor_price_obj = await self.gamivo_client.calculate_seller_price(offer_id, competitor_price_raw)
                competitor_price = competitor_price_obj.seller_price
                precalculated_prices[competitor_seller] = competitor_price

                final_price = self._calculate_final_price(
                    competitor_price,
                    payload.min_change_price,
                    payload.max_change_price,
                    payload.rounding_precision,
                    min_price,
                    max_price
                )

            top_sellers_list = offer_analysis["top_sellers_for_log"]
            log_lines_sellers = await self._format_top_sellers_log(
                top_sellers_list,
                offer_id,
                precalculated_prices
            )

            # --- PHASE 2: DECIDE ---
            should_update = False
            log_msg_if_skipped = ""

            if payload.get_mode == 1 or my_current_price > final_price:
                should_update = True
            else:
                should_update = False
                if min_price > my_current_price:
                    should_update = True
                    final_price = min_price
                    log_msg_if_skipped = "\n".join([
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá thấp hơn giá min, cập nhật lên min_price",
                        f"Giá hiện tại: {my_current_price:.2f} | Giá mục tiêu: {competitor_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}",
                        *log_lines_sellers
                    ])
                else:
                    log_msg_if_skipped = "\n".join([
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Không cập nhật",
                        f"Giá hiện tại: {my_current_price:.2f} | Giá mục tiêu: {competitor_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}",
                        *log_lines_sellers
                    ])

            # --- PHASE 3: ACTION ---
            if should_update:
                status, response = await self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)

                if status == 200:
                    log_lines = [
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá đã cập nhật thành công; Price = {final_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}"
                    ]
                    log_lines.extend(log_lines_sellers)
                    log_msg = "\n".join(log_lines)

                    logging.info(f"Successfully processed row {payload.sheet_row_num}. Log details:\n{log_msg}")
                    self._add_log(payload.sheet_row_num, log_msg, 'C')
                else:
                    log_msg = f"Failed to process: {response}"
                    self._add_log(payload.sheet_row_num, log_msg, 'E')
                    raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
            else:
                logging.info(
                    f"Skipping update for row {payload.sheet_row_num} (Mode 2). Log details:\n{log_msg_if_skipped}")
                self._add_log(payload.sheet_row_num, log_msg_if_skipped, 'C')

        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')
            raise # Ném lỗi ra ngoài để asyncio.gather(return_exceptions=True) bắt
        finally:
            self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    # --- CÁC HÀM LOGGING (Không thay đổi) ---

    async def _get_top_sellers_log(self, product_id: int, offer_id: int) -> List[str]:
        try:
            log_offers = await self.gamivo_client.get_product_offers(product_id)
            log_sorted_offers = sorted(log_offers, key=lambda o: o.retail_price)[:4]
            return await self._format_top_sellers_log(log_sorted_offers, offer_id, {})
        except Exception as e:
            logging.warning(f"Could not generate top sellers log for product {product_id}: {e}")
            return [" - Error fetching top sellers."]

    def _add_log(self, row_num: int, message: str, column: str):
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    async def _flush_logs(self):
        if not self.log_buffer: return
        logs_to_flush = self.log_buffer.copy()
        self.log_buffer = []

        try:
            async with self.gsheet_lock:
                await asyncio.to_thread(
                    self.gsheet_client.batch_update, self.config.main_sheet_id, logs_to_flush
                )
            logging.info(f"Wrote {len(logs_to_flush)} log entries to the sheet.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")
            # self.log_buffer.extend(logs_to_flush) # Cân nhắc thêm log lại

    # --- HÀM WRAPPER TASK (Không thay đổi) ---

    async def _process_payload_with_limit(self, payload: Payload, semaphore: asyncio.Semaphore, config_cache: dict):
        """
        Một hàm bao bọc (wrapper) để xử lý payload trong sự kiểm soát của Semaphore.
        """
        async with semaphore:
            start_time = monotonic()
            logging.info(f"Processing '{payload.product_name}' from row {payload.sheet_row_num}...")

            # Truyền config_cache vào
            await self._process_single_payload(payload, config_cache)

            end_time = monotonic()
            duration = end_time - start_time
            logging.info(f"Finished '{payload.product_name}' in {duration:.2f} seconds.")

    # --- HÀM RUN CHÍNH (ĐÃ SỬA) ---
    async def run(self):
        """Vòng lặp xử lý chính với BATCH READ đa file và CHUẨN HÓA KEY."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            config_cache: Dict[str, List[List[str]]] = {}

            try:
                # 1. Lấy danh sách payload (1 API Read)
                range_to_fetch = f"{self.config.main_sheet_name}!A{self.config.start_row}:AC"
                async with self.gsheet_lock:
                    sheet_data = await asyncio.to_thread(
                        self.gsheet_client.get_data, self.config.main_sheet_id, range_to_fetch
                    )

                payloads = [Payload.from_row(row_data, self.config.start_row + i) for i, row_data in enumerate(sheet_data)]
                valid_payloads = [p for p in payloads if p and p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                    await asyncio.sleep(float(self.config.loop_delay_seconds))
                    continue

                # 2. THU THẬP & NHÓM CÁC RANGES THEO SPREADSHEET_ID
                ranges_by_spreadsheet: Dict[str, set] = defaultdict(set)

                bl_loc = valid_payloads[0].blacklist_location
                if all([bl_loc.sheet_id, bl_loc.sheet_name, bl_loc.cell]):
                    ranges_by_spreadsheet[bl_loc.sheet_id].add(f"{bl_loc.sheet_name}!{bl_loc.cell}")

                for p in valid_payloads:
                    locs = [p.min_price_location, p.max_price_location, p.stock_location]
                    for loc in locs:
                        if loc.sheet_id and loc.sheet_name and loc.cell:
                             ranges_by_spreadsheet[loc.sheet_id].add(f"{loc.sheet_name}!{loc.cell}")

                # 3. GỌI BATCH GET CHO TỪNG SPREADSHEET (chạy song song)
                logging.info(f"Fetching configs from {len(ranges_by_spreadsheet)} unique spreadsheets.")

                fetch_tasks = []
                for sheet_id, ranges in ranges_by_spreadsheet.items():
                    fetch_tasks.append(
                        self._execute_batch_get(sheet_id, list(ranges))
                    )

                all_results = await asyncio.gather(*fetch_tasks)

                # 4. XỬ LÝ KẾT QUẢ VÀO CACHE DUY NHẤT (ĐÃ SỬA)
                config_cache = {}
                for (sheet_id, value_ranges) in all_results:
                    for vr in value_ranges:
                        range_name_from_google = vr.get('range') # Ví dụ: 'Data'!C2 hoặc Data!C2
                        values = vr.get('values')

                        if not (range_name_from_google and values):
                            continue

                        # --- LOGIC CHUẨN HÓA MỚI ---
                        # Chuẩn hóa range name, loại bỏ dấu '
                        # Ví dụ: 'Data'!C2 -> Data!C2
                        match = re.match(r"'(.*)'!(.*)", range_name_from_google)
                        if match:
                            # Tên sheet có dấu ' (ví dụ: 'My Sheet'!A1)
                            normalized_range = f"{match.group(1)}!{match.group(2)}"
                        else:
                            # Tên sheet không có dấu ' (ví dụ: Data!A1)
                            normalized_range = range_name_from_google

                        # Key đã được chuẩn hóa (ví dụ: "1L7...:Data!C2")
                        cache_key = f"{sheet_id}:{normalized_range}"
                        config_cache[cache_key] = values
                        # --- KẾT THÚC LOGIC MỚI ---

                # 5. Cập nhật blacklist từ cache (ĐÃ SỬA)
                # Build key chuẩn hóa cho blacklist (không có dấu ')
                bl_range_name = f"{bl_loc.sheet_name}!{bl_loc.cell}"
                blacklist_key = f"{bl_loc.sheet_id}:{bl_range_name}"

                blacklist_data = config_cache.get(blacklist_key, [])
                self.blacklist = [item for sublist in blacklist_data for item in sublist if item]
                logging.info(f"Fetched {len(self.blacklist)} sellers for the blacklist.")

                # 6. Chuẩn bị và chạy tasks
                num_workers = int(self.config.workers)
                semaphore = asyncio.Semaphore(num_workers)
                logging.info(f"Running {len(valid_payloads)} tasks with {num_workers} workers.")

                tasks = []
                for payload in valid_payloads:
                    tasks.append(self._process_payload_with_limit(payload, semaphore, config_cache))

                results = await asyncio.gather(*tasks, return_exceptions=True)

                for res in results:
                    if isinstance(res, Exception):
                        logging.error(f"A task failed permanently: {res.__class__.__name__}: {res}")

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)

            finally:
                await self._flush_logs()

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            await asyncio.sleep(float(self.config.loop_delay_seconds))

    # --- HÀM LOGGING CUỐI CÙNG (Không thay đổi) ---

    async def _format_top_sellers_log(self,
                                      log_sorted_offers: List[OfferDetails],
                                      offer_id: int,
                                      precalculated_prices: Dict[str, float]) -> List[str]:
        log_lines = ["----Top Sellers----"]
        seller_prefixes = ["1st", "2nd", "3rd", "4th"]
        final_prices = precalculated_prices.copy()
        tasks_to_run = []
        seller_task_map = []

        for offer in log_sorted_offers:
            if offer.seller_name not in final_prices:
                tasks_to_run.append(
                    self.gamivo_client.calculate_seller_price(offer_id, offer.retail_price)
                )
                seller_task_map.append(offer.seller_name)
        try:
            if tasks_to_run:
                calculated_price_objects = await asyncio.gather(*tasks_to_run)
                for i, seller_name in enumerate(seller_task_map):
                    final_prices[seller_name] = calculated_price_objects[i].seller_price

            for i, offer in enumerate(log_sorted_offers):
                calculated_price = final_prices.get(offer.seller_name)
                price_str = f"{calculated_price:.2f}" if calculated_price is not None else "N/A"
                retail_price = offer.retail_price
                log_lines.append(
                    f" - {seller_prefixes[i]} Seller: {offer.seller_name} - Price: {retail_price} ({price_str})")

        except Exception as e:
            logging.warning(f"Could not generate formatted top sellers log: {e}")
            log_lines.append(" - Error formatting top sellers.")

        return log_lines