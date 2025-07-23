# logic/price_processor.py
"""
The core business logic orchestrator for the price update process.
"""
import logging
from time import sleep, monotonic
from datetime import datetime
from typing import List, Tuple, Optional, Dict

# Assuming these components are in their respective paths
from utils.config import Config
from clients.google_sheets_client import GoogleSheetsClient
# IMPORTANT: Make sure to import the updated GamivoClient
from clients.gamivo_client import GamivoClient, GamivoAPIError, OfferDetails
from models.sheet_models import Payload


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

    def _fetch_blacklist(self, payload: Payload):
        """Fetches the blacklist from the sheet location specified in the payload."""
        loc = payload.blacklist_location
        if not all([loc.sheet_id, loc.sheet_name, loc.cell]):
            logging.warning("Blacklist location is not fully configured. Skipping fetch.")
            self.blacklist = []
            return

        range_name = f"{loc.sheet_name}!{loc.cell}"
        blacklist_data = self.gsheet_client.get_data(loc.sheet_id, range_name)
        self.blacklist = [item for sublist in blacklist_data for item in sublist if item]
        logging.info(f"Fetched {len(self.blacklist)} sellers for the blacklist.")

    def _get_price_boundaries(self, payload: Payload) -> Tuple[float, float, int]:
        min_loc, max_loc, stock_loc = payload.min_price_location, payload.max_price_location, payload.stock_location
        min_price_raw = self.gsheet_client.get_data(min_loc.sheet_id, f"{min_loc.sheet_name}!{min_loc.cell}")[0][0]
        max_price_raw = self.gsheet_client.get_data(max_loc.sheet_id, f"{max_loc.sheet_name}!{max_loc.cell}")[0][0]
        stock_raw = self.gsheet_client.get_data(stock_loc.sheet_id, f"{stock_loc.sheet_name}!{stock_loc.cell}")[0][0]
        return float(min_price_raw), float(max_price_raw), int(stock_raw)

    def _analyze_offers(self, product_id: int, min_price: float) -> Dict[str, any]:
        offers = self.gamivo_client.get_product_offers(product_id)
        sorted_offers = sorted(offers, key=lambda o: o.retail_price)
        analysis = {"valid_competitor": None, "top_sellers_for_log": sorted_offers[:4], "sellers_below_min": []}
        for offer in sorted_offers:
            if offer.seller_name not in self.blacklist:
                analysis["valid_competitor"] = offer
                break
        for offer in sorted_offers:
            if offer.retail_price < min_price:
                analysis["sellers_below_min"].append(offer)
        return analysis

    def _calculate_final_price(self, current_price, target_price, min_change, max_change, rounding, min_price,
        max_price):
        target_price = max(target_price, min_price)
        target_price = min(target_price, max_price)
        if current_price == 0: return round(max(min_price, target_price - min_change), rounding)
        if current_price == target_price:
            while round(current_price, rounding) >= target_price: current_price -= min_change
            return round(max(min_price, current_price), rounding)
        if current_price < target_price: return round(max(min_price, target_price - min_change), rounding)
        if current_price > target_price:
            if target_price < min_price: return min_price
            while round(current_price - target_price, rounding) >= max_change and round(current_price - max_change,
                rounding) >= min_price: current_price -= max_change
            while round(current_price - target_price, rounding) >= min_change and round(current_price - min_change,
                rounding) >= min_price: current_price -= min_change
            return round(max(current_price, min_price), rounding)
        return round(max(current_price, min_price), rounding)

    def _process_single_payload(self, payload: Payload):
        try:
            min_price, max_price, stock = self._get_price_boundaries(payload)
            offer_id = self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
            if not offer_id: raise Exception(f"Offer ID not found for product {payload.product_compare_id}")

            my_offer_data = self.gamivo_client.retrieve_my_offer(offer_id)
            my_current_price = self.gamivo_client.calculate_seller_price(offer_id,
                my_offer_data['retail_price']).seller_price
            offer_analysis = self._analyze_offers(payload.product_compare_id, min_price)
            valid_competitor = offer_analysis["valid_competitor"]

            log_msg, final_price = "", max_price
            if valid_competitor is None:
                log_msg = f"No valid competitor found. Setting price to MAX: {final_price:.2f}"
            else:
                competitor_seller, competitor_price_raw = valid_competitor.seller_name, valid_competitor.retail_price
                competitor_price = self.gamivo_client.calculate_seller_price(offer_id,
                    competitor_price_raw).seller_price
                final_price = self._calculate_final_price(my_current_price, competitor_price, payload.min_change_price,
                    payload.max_change_price, payload.rounding_precision, min_price, max_price)

                log_lines = [
                    f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá đã cập nhật thành công; Price = {final_price:.2f}; Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}",
                    "----Top Sellers----"]
                for i, offer in enumerate(offer_analysis["top_sellers_for_log"]):
                    log_lines.append(
                        f" - {['1st', '2nd', '3rd', '4th'][i]} Seller: {offer.seller_name} - Price: {offer.retail_price:.2f}")
                if offer_analysis["sellers_below_min"]:
                    log_lines.append("----Seller price lower than min-----")
                    for offer in offer_analysis["sellers_below_min"]:
                        log_lines.append(f"{offer.seller_name} - Price: {offer.retail_price:.2f}")
                log_msg = "\n".join(log_lines)

            status, response = self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)
            if status == 200:
                # Ghi log chi tiết ra console khi xử lý thành công
                logging.info(f"Successfully processed row {payload.sheet_row_num}. Log details:\n{log_msg}")
                self._add_log(payload.sheet_row_num, log_msg, 'C')
            else:
                raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')
        finally:
            self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    def _add_log(self, row_num: int, message: str, column: str):
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    def _flush_logs(self):
        if not self.log_buffer: return
        try:
            self.gsheet_client.batch_update(self.config.main_sheet_id, self.log_buffer)
            # Log message is now more specific
            logging.info(f"Wrote {len(self.log_buffer)} log entries to the sheet.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")
        finally:
            self.log_buffer = []

    def run(self):
        """The main processing loop using a configured start_row."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            try:
                # Open DB connection at the start of the cycle
                self.gamivo_client.db_client.create_connection()

                range_to_fetch = f"{self.config.main_sheet_name}!A{self.config.start_row}:W"
                sheet_data = self.gsheet_client.get_data(self.config.main_sheet_id, range_to_fetch)

                payloads = [Payload.from_row(row_data, self.config.start_row + i) for i, row_data in
                            enumerate(sheet_data)]
                valid_payloads = [p for p in payloads if p and p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                else:
                    self._fetch_blacklist(valid_payloads[0])

                logging.info(f"Found {len(valid_payloads)} enabled products to process.")
                for payload in valid_payloads:
                    start_time = monotonic()
                    logging.info(f"Processing '{payload.product_name}' from row {payload.sheet_row_num}...")
                    self._process_single_payload(payload)

                    # Flush logs to the sheet immediately after processing one payload
                    self._flush_logs()

                    end_time = monotonic()
                    duration = end_time - start_time
                    logging.info(f"Finished '{payload.product_name}' in {duration:.2f} seconds.")

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
                # Attempt to flush any remaining logs in case of an error mid-payload
                self._flush_logs()

            finally:
                # This block ALWAYS runs, ensuring the DB connection is closed.
                self.gamivo_client.db_client.close_connection()

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            sleep(self.config.loop_delay_seconds)
