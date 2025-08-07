# logic/price_processor.py
"""
The core business logic orchestrator for the price update process.
"""
import logging
import random
from datetime import datetime
from time import sleep, monotonic
from typing import List, Tuple, Optional, Dict

# IMPORTANT: Make sure to import the updated GamivoClient
from clients.gamivo_client import GamivoClient, GamivoAPIError
from clients.google_sheets_client import GoogleSheetsClient
from models.sheet_models import Payload
# Assuming these components are in their respective paths
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

    def _analyze_offers(self, product_id: int, my_seller_name: Optional[str]) -> Dict[str, any]:
        """
        Analyzes product offers to find the valid competitor and top sellers for logging.
        It explicitly excludes `my_seller_name` from competitor consideration.
        """
        offers = self.gamivo_client.get_product_offers(product_id)
        sorted_offers = sorted(offers, key=lambda o: o.retail_price)
        analysis = {"valid_competitor": None, "top_sellers_for_log": sorted_offers[:4]}

        # Find the first valid competitor
        for offer in sorted_offers:
            # A competitor must not be me and must not be in the blacklist
            if offer.seller_name != my_seller_name and offer.seller_name not in self.blacklist:
                analysis["valid_competitor"] = offer
                break
        return analysis

    def _calculate_final_price(self, target_price: float, min_change: float, max_change: float, rounding: int, min_price: float,
                               max_price: float) -> float:
        """
        Calculates the final price with a simplified and direct logic.
        The state of the current price is irrelevant; only the target matters.
        """
        # 1. Determine the ideal price to undercut the competitor.
        random_price = random.uniform(min_change, max_change)
        ideal_price = target_price - random_price

        # 2. Ensure the price is not lower than our minimum allowed price.
        price_after_min_check = max(min_price, ideal_price)

        # 3. Ensure the price does not exceed our maximum allowed price.
        final_price = min(max_price, price_after_min_check)

        # 4. Return the rounded final price.
        return round(final_price, rounding)

    def _process_single_payload(self, payload: Payload):
        try:
            min_price, max_price, stock = self._get_price_boundaries(payload)
            offer_id = self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
            if not offer_id: raise Exception(f"Offer ID not found for product {payload.product_compare_id}")

            my_offer_data = self.gamivo_client.retrieve_my_offer(offer_id)
            my_seller_name = my_offer_data.get('seller_name')

            offer_analysis = self._analyze_offers(payload.product_compare_id, my_seller_name)
            valid_competitor = offer_analysis["valid_competitor"]

            log_msg = ""
            final_price = max_price

            if valid_competitor is None:
                # If there's no one to compete with, set price to max
                final_price = max_price
                log_msg = f"No valid competitor found. Setting price to MAX: {final_price:.2f}"
            else:
                competitor_seller = valid_competitor.seller_name
                competitor_price_raw = valid_competitor.retail_price
                competitor_price = self.gamivo_client.calculate_seller_price(offer_id,
                                                                             competitor_price_raw).seller_price

                final_price = self._calculate_final_price(
                    competitor_price,
                    payload.min_change_price,
                    payload.max_change_price,
                    payload.rounding_precision,
                    min_price,
                    max_price
                )
            status, response = self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)
            if status == 200:
                log_lines = [
                    f"{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}: Giá đã cập nhật thành công; Price = {final_price:.2f}; Pricemin = {min_price}, Pricemax = {max_price}, GiaSosanh = {competitor_price:.2f} - Seller: {competitor_seller}",
                    "----Top Sellers----"]
                seller_prefixes = ["1st", "2nd", "3rd", "4th"]
                log_offers = self.gamivo_client.get_product_offers(payload.product_compare_id)
                log_sorted_offers = sorted(log_offers, key=lambda o: o.retail_price)
                offer_analysis["top_sellers_for_log"] = log_sorted_offers[:4]
                for i, offer in enumerate(offer_analysis["top_sellers_for_log"]):
                    calculated_price = self.gamivo_client.calculate_seller_price(offer_id,
                                                                                 offer.retail_price).seller_price
                    retail_price = offer.retail_price
                    log_lines.append(
                        f" - {seller_prefixes[i]} Seller: {offer.seller_name} - Price: {retail_price} ({calculated_price:.2f})")

                log_msg = "\n".join(log_lines)
                logging.info(f"Successfully processed row {payload.sheet_row_num}. Log details:\n{log_msg}")
                self._add_log(payload.sheet_row_num, log_msg, 'C')
            else:
                log_msg = f"Failed to process: {response}"
                self._add_log(payload.sheet_row_num, log_msg, 'E')
                raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')
            logging.error(f"Retrying in {self.config.retries_time_sleep} seconds...")
            sleep(int(self.config.retries_time_sleep))
        finally:
            self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    def _add_log(self, row_num: int, message: str, column: str):
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    def _flush_logs(self):
        if not self.log_buffer: return
        try:
            self.gsheet_client.batch_update(self.config.main_sheet_id, self.log_buffer)
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
                    self._flush_logs()
                    end_time = monotonic()
                    duration = end_time - start_time
                    logging.info(f"Finished '{payload.product_name}' in {duration:.2f} seconds.")
                    sleep(self.config.retries_time_sleep)

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
                self._flush_logs()

            finally:
                self.gamivo_client.db_client.close_connection()

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            sleep(self.config.loop_delay_seconds)
