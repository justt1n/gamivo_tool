"""
The core business logic orchestrator for the price update process.
ASYNCHRONOUS version with concurrency limiting and jitter.
"""
import asyncio  # Import asyncio
import logging
import random
from datetime import datetime
from time import monotonic
from typing import List, Tuple, Optional, Dict

from clients.gamivo_client import GamivoClient, GamivoAPIError
from clients.google_sheets_client import GoogleSheetsClient
from models.gamivo_models import OfferDetails
from models.sheet_models import Payload
from utils.config import Config


class PriceProcessor:
    """
    Orchestrates the entire process of fetching data, processing prices,
    and logging results.
    """

    def __init__(self, config: Config):
        self.config = config
        self.gsheet_client = GoogleSheetsClient(config.google_key_path)
        # Khởi tạo client bất đồng bộ (cache được load đồng bộ trong __init__)
        self.gamivo_client = GamivoClient(
            api_key=config.gamivo_api_key,
            db_path=config.db_path
        )
        self.blacklist: List[str] = []
        self.log_buffer: List[dict] = []
        self.gsheet_lock = asyncio.Lock()

    async def _fetch_blacklist(self, payload: Payload):
        """Fetches the blacklist from the sheet location specified in the payload."""
        loc = payload.blacklist_location
        if not all([loc.sheet_id, loc.sheet_name, loc.cell]):
            logging.warning("Blacklist location is not fully configured. Skipping fetch.")
            self.blacklist = []
            return

        range_name = f"{loc.sheet_name}!{loc.cell}"

        # !!! THÊM KHÓA VÀO ĐÂY !!!
        async with self.gsheet_lock:
            # Chạy code đồng bộ (blocking I/O) trong một thread riêng
            blacklist_data = await asyncio.to_thread(
                self.gsheet_client.get_data, loc.sheet_id, range_name
            )

        self.blacklist = [item for sublist in blacklist_data for item in sublist if item]
        logging.info(f"Fetched {len(self.blacklist)} sellers for the blacklist.")

    def _sync_get_price_boundaries(self, payload: Payload) -> Tuple[float, float, int]:
        """
        Hàm helper ĐỒNG BỘ để lấy 3 giá trị từ sheet.
        Hàm này sẽ chạy trong một thread riêng.
        """
        min_loc, max_loc, stock_loc = payload.min_price_location, payload.max_price_location, payload.stock_location

        min_price_raw = self.gsheet_client.get_data(min_loc.sheet_id, f"{min_loc.sheet_name}!{min_loc.cell}")[0][0]
        max_price_raw = self.gsheet_client.get_data(max_loc.sheet_id, f"{max_loc.sheet_name}!{max_loc.cell}")[0][0]
        stock_raw = self.gsheet_client.get_data(stock_loc.sheet_id, f"{stock_loc.sheet_name}!{stock_loc.cell}")[0][0]

        return float(min_price_raw), float(max_price_raw), int(stock_raw)

    async def _get_price_boundaries(self, payload: Payload) -> Tuple[float, float, int]:
        async with self.gsheet_lock:
            return await asyncio.to_thread(
                self._sync_get_price_boundaries, payload
            )

    async def _analyze_offers(self, product_id: int, my_seller_name: Optional[str]) -> Dict[str, any]:
        """
        Analyzes product offers to find the valid competitor and top sellers for logging.
        """
        # Dùng await cho lệnh gọi API
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
        # Hàm này chỉ tính toán (CPU-bound), không cần async
        random_price = random.uniform(min_change, max_change)
        ideal_price = target_price - random_price
        price_after_min_check = max(min_price, ideal_price)
        final_price = min(max_price, price_after_min_check)
        return round(final_price, rounding)

    async def _process_single_payload(self, payload: Payload):
        """Xử lý logic cho một payload duy nhất."""
        # Dùng để lưu các giá đã tính (seller_name -> calculated_price)
        precalculated_prices: Dict[str, float] = {}
        log_lines_sellers = []  # Dùng để lưu log sellers
        try:
            # --- PHASE 1: SETUP & CALCULATE ---
            min_price, max_price, stock = await self._get_price_boundaries(payload)

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

                # Tính giá đối thủ và LƯU LẠI
                competitor_price_obj = await self.gamivo_client.calculate_seller_price(offer_id, competitor_price_raw)
                competitor_price = competitor_price_obj.seller_price

                # Lưu vào dictionary
                precalculated_prices[competitor_seller] = competitor_price

                final_price = self._calculate_final_price(
                    competitor_price,
                    payload.min_change_price,
                    payload.max_change_price,
                    payload.rounding_precision,
                    min_price,
                    max_price
                )

            # !!! TỐI ƯU HÓA: TÍNH TOÁN LOG NGAY BÂY GIỜ !!!
            # Truyền precalculated_prices vào
            top_sellers_list = offer_analysis["top_sellers_for_log"]
            log_lines_sellers = await self._format_top_sellers_log(
                top_sellers_list,
                offer_id,
                precalculated_prices
            )

            # --- PHASE 2: DECIDE ---
            should_update = False
            log_msg_if_skipped = ""

            if payload.get_mode == 1:
                should_update = True
            else:
                if my_current_price > final_price:
                    should_update = True
                else:
                    # Dùng log đã được tính toán ở trên
                    log_lines = [
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Không cập nhật",
                        f"Giá hiện tại: {my_current_price:.2f} | Giá mục tiêu: {competitor_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}"
                    ]
                    log_lines.extend(log_lines_sellers)  # Thêm log sellers
                    log_msg_if_skipped = "\n".join(log_lines)

            # --- PHASE 3: ACTION ---
            if should_update:
                status, response = await self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)

                if status == 200:
                    log_lines = [
                        f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá đã cập nhật thành công; Price = {final_price:.2f}",
                        f"Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}"
                    ]
                    # Dùng log sellers đã tính toán
                    log_lines.extend(log_lines_sellers)
                    log_msg = "\n".join(log_lines)

                    logging.info(f"Successfully processed row {payload.sheet_row_num}. Log details:\n{log_msg}")
                    self._add_log(payload.sheet_row_num, log_msg, 'C')
                else:
                    log_msg = f"Failed to process: {response}"
                    self._add_log(payload.sheet_row_num, log_msg, 'E')
                    raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
            else:
                # Ghi log (đã được tạo ở Phase 2)
                logging.info(
                    f"Skipping update for row {payload.sheet_row_num} (Mode 2). Log details:\n{log_msg_if_skipped}")
                self._add_log(payload.sheet_row_num, log_msg_if_skipped, 'C')

        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')
            raise
        finally:
            self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    async def _get_top_sellers_log(self, product_id: int, offer_id: int) -> List[str]:
        """
        Helper method to FETCH and format the top 4 sellers for logging.
        *** This function CALLS APIs (use only after an update). ***
        """
        try:
            # Lấy offers (bất đồng bộ)
            log_offers = await self.gamivo_client.get_product_offers(product_id)
            log_sorted_offers = sorted(log_offers, key=lambda o: o.retail_price)[:4]

            # Gọi hàm format mới
            return await self._format_top_sellers_log(log_sorted_offers, offer_id)

        except Exception as e:
            logging.warning(f"Could not generate top sellers log for product {product_id}: {e}")
            return [" - Error fetching top sellers."]

    def _add_log(self, row_num: int, message: str, column: str):
        # Hàm này chỉ thêm vào buffer (memory), không I/O, nên giữ nguyên đồng bộ
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    async def _flush_logs(self):
        if not self.log_buffer: return

        # Tạo bản sao buffer và xóa buffer gốc ngay lập tức
        logs_to_flush = self.log_buffer.copy()
        self.log_buffer = []

        try:
            # !!! THÊM KHÓA VÀO ĐÂY !!!
            async with self.gsheet_lock:
                # Chạy I/O đồng bộ trong thread
                await asyncio.to_thread(
                    self.gsheet_client.batch_update, self.config.main_sheet_id, logs_to_flush
                )
            logging.info(f"Wrote {len(logs_to_flush)} log entries to the sheet.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")
            # (Cân nhắc): Thêm logs_to_flush trở lại self.log_buffer để thử lại lần sau
            # self.log_buffer.extend(logs_to_flush)

    async def _process_payload_with_limit(self, payload: Payload, semaphore: asyncio.Semaphore):
        """
        Một hàm bao bọc (wrapper) để xử lý payload trong sự kiểm soát của Semaphore.
        Thêm JITTER để tránh lỗi SSL.
        """
        # Chờ để "vào cổng" (giới hạn số tác vụ đồng thời)
        async with semaphore:
            start_time = monotonic()
            logging.info(
                f"Processing '{payload.product_name}' from row {payload.sheet_row_num}...")

            # !!! KHÔNG CÒN try...except ở đây !!!
            # Hãy để lỗi (sau khi retry) tự nổi lên
            # Logic xử lý lỗi chi tiết đã nằm trong _process_single_payload
            await self._process_single_payload(payload)

            end_time = monotonic()
            duration = end_time - start_time
            logging.info(f"Finished '{payload.product_name}' in {duration:.2f} seconds.")

            # Sleep sau khi xử lý (sleep này giờ cũng chạy song song)
            # await asyncio.sleep(float(self.config.retries_time_sleep))

    async def run(self):
        """The main asynchronous processing loop with CONCURRENCY LIMIT."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            try:
                # 1. Khởi tạo Semaphore (lấy giới hạn từ config)
                num_workers = int(self.config.workers)
                semaphore = asyncio.Semaphore(num_workers)

                # 2. Lấy dữ liệu (blocking I/O trong thread)
                range_to_fetch = f"{self.config.main_sheet_name}!A{self.config.start_row}:AC"

                # !!! THÊM KHÓA VÀO ĐÂY !!!
                async with self.gsheet_lock:
                    sheet_data = await asyncio.to_thread(
                        self.gsheet_client.get_data, self.config.main_sheet_id, range_to_fetch
                    )

                payloads = [Payload.from_row(row_data, self.config.start_row + i) for i, row_data in
                            enumerate(sheet_data)]
                valid_payloads = [p for p in payloads if p and p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                else:
                    # Lấy blacklist (blocking I/O trong thread)
                    await self._fetch_blacklist(valid_payloads[0])

                logging.info(
                    f"Found {len(valid_payloads)} enabled products. Running with {num_workers} concurrent tasks.")

                # 3. Tạo danh sách các tác vụ
                tasks = []
                for payload in valid_payloads:
                    tasks.append(self._process_payload_with_limit(payload, semaphore))

                # 4. Chạy tất cả các tác vụ và THU LẠI EXCEPTION
                # Thêm return_exceptions=True để gather không dừng lại khi có lỗi
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # (Tùy chọn) Ghi log các lỗi đã xảy ra sau khi gather
                for res in results:
                    if isinstance(res, Exception):
                        # Lỗi này đã được log chi tiết bên trong _process_single_payload
                        # Chúng ta chỉ log một thông báo chung ở đây
                        logging.error(f"A task failed permanently after all retries: {res.__class__.__name__}: {res}")

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)

            finally:
                # 5. Xả log MỘT LẦN sau khi tất cả tác vụ hoàn thành
                await self._flush_logs()

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            await asyncio.sleep(float(self.config.loop_delay_seconds))

    async def _format_top_sellers_log(self,
                                      log_sorted_offers: List[OfferDetails],
                                      offer_id: int,
                                      precalculated_prices: Dict[str, float]) -> List[str]:
        """
        Helper method to FORMAT the top 4 sellers log.
        *** This function only calls APIs for sellers NOT in precalculated_prices. ***
        """
        log_lines = ["----Top Sellers----"]
        seller_prefixes = ["1st", "2nd", "3rd", "4th"]

        # Dictionary để lưu kết quả cuối cùng
        final_prices = precalculated_prices.copy()

        # Danh sách các task (API call) cần chạy
        tasks_to_run = []
        # Dùng để map kết quả từ gather về đúng seller
        seller_task_map = []

        for offer in log_sorted_offers:
            # Chỉ chạy API nếu chúng ta CHƯA tính giá cho seller này
            if offer.seller_name not in final_prices:
                tasks_to_run.append(
                    self.gamivo_client.calculate_seller_price(offer_id, offer.retail_price)
                )
                seller_task_map.append(offer.seller_name)

        try:
            # Chạy song song các API call CÒN LẠI
            if tasks_to_run:
                calculated_price_objects = await asyncio.gather(*tasks_to_run)
                # Map kết quả trở lại
                for i, seller_name in enumerate(seller_task_map):
                    final_prices[seller_name] = calculated_price_objects[i].seller_price

            # Bây giờ, tạo log từ dictionary final_prices
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
