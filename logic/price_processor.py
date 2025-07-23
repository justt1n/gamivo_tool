# logic/price_processor.py
"""
The core business logic orchestrator for the price update process.
"""
import logging
from time import sleep
from datetime import datetime
from typing import List, Tuple, Optional

# Assuming these components are in their respective paths
from utils.config import Config
from clients.google_sheets_client import GoogleSheetsClient
from clients.gamivo_client import GamivoClient, GamivoAPIError
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
        # Flatten the 2D array returned by the API and filter out empty strings
        self.blacklist = [item for sublist in blacklist_data for item in sublist if item]
        logging.info(f"Fetched {len(self.blacklist)} sellers for the blacklist.")

    def _get_price_boundaries(self, payload: Payload) -> Tuple[float, float, int]:
        # This method remains unchanged
        min_loc, max_loc, stock_loc = payload.min_price_location, payload.max_price_location, payload.stock_location
        min_price_raw = self.gsheet_client.get_data(min_loc.sheet_id, f"{min_loc.sheet_name}!{min_loc.cell}")[0][0]
        max_price_raw = self.gsheet_client.get_data(max_loc.sheet_id, f"{max_loc.sheet_name}!{max_loc.cell}")[0][0]
        stock_raw = self.gsheet_client.get_data(stock_loc.sheet_id, f"{stock_loc.sheet_name}!{stock_loc.cell}")[0][0]
        return float(min_price_raw), float(max_price_raw), int(stock_raw)

    def _find_valid_competitor(self, product_id: int, min_price: float) -> Tuple[Optional[str], Optional[float]]:
        # This method remains unchanged
        offers = self.gamivo_client.get_product_offers(product_id)
        for offer in sorted(offers, key=lambda o: o.retail_price):
            if offer.seller_name not in self.blacklist:
                return offer.seller_name, offer.retail_price
        return None, None

    def _calculate_final_price(self, current_price, target_price, min_change, max_change, rounding, min_price,
        max_price):
        # This method remains unchanged
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
        """Processes a single payload, using the row number from the payload for logging."""
        try:
            min_price, max_price, stock = self._get_price_boundaries(payload)
            offer_id = self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
            if not offer_id: raise Exception(f"Offer ID not found for product {payload.product_compare_id}")

            my_offer_data = self.gamivo_client.retrieve_my_offer(offer_id)
            my_current_price = self.gamivo_client.calculate_seller_price(offer_id,
                my_offer_data['retail_price']).seller_price
            competitor_seller, competitor_price_raw = self._find_valid_competitor(payload.product_compare_id, min_price)

            if competitor_seller is None or competitor_price_raw is None:
                final_price, log_msg = max_price, f"No valid competitor. Set to MAX: {max_price:.2f}"
            else:
                competitor_price = self.gamivo_client.calculate_seller_price(offer_id,
                    competitor_price_raw).seller_price
                final_price = self._calculate_final_price(my_current_price, competitor_price, payload.min_change_price,
                    payload.max_change_price, payload.rounding_precision, min_price, max_price)
                log_msg = f"Success. Price={final_price:.2f}; Competing with '{competitor_seller}' ({competitor_price:.2f})"

            status, response = self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)
            if status == 200:
                self._add_log(payload.sheet_row_num, log_msg, 'C')
            else:
                raise GamivoAPIError(f"Update failed: {response.get('message', 'Unknown')}", status)
        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}' on row {payload.sheet_row_num}: {e}")
            self._add_log(payload.sheet_row_num, f"Error: {e}", 'E')
        finally:
            self._add_log(payload.sheet_row_num, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    def _add_log(self, row_num: int, message: str, column: str):
        """Adds a log entry to the buffer using the exact sheet row number."""
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    def _flush_logs(self):
        # This method remains unchanged
        if not self.log_buffer: return
        try:
            self.gsheet_client.batch_update(self.config.main_sheet_id, self.log_buffer)
            logging.info(f"Successfully wrote {len(self.log_buffer)} log entries.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")
        finally:
            self.log_buffer = []

    def run(self):
        """The main processing loop using a configured start_row."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            try:
                # Fetch data starting from the configured row
                range_to_fetch = f"{self.config.main_sheet_name}!A{self.config.start_row}:W"
                sheet_data = self.gsheet_client.get_data(self.config.main_sheet_id, range_to_fetch)

                # Create Payload objects with the correct sheet row number
                payloads = []
                for i, row_data in enumerate(sheet_data):
                    # Calculate the actual row number in the sheet
                    actual_row_num = self.config.start_row + i
                    payload = Payload.from_row(row_data, actual_row_num)
                    if payload:
                        payloads.append(payload)

                valid_payloads = [p for p in payloads if p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                else:
                    # Fetch blacklist once using the first valid payload's config
                    self._fetch_blacklist(valid_payloads[0])

                logging.info(f"Found {len(valid_payloads)} enabled products to process.")
                for payload in valid_payloads:
                    logging.info(f"Processing '{payload.product_name}' from row {payload.sheet_row_num}...")
                    self._process_single_payload(payload)

                self._flush_logs()

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
                self._flush_logs()

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            sleep(self.config.loop_delay_seconds)
