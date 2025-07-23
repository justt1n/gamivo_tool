# logic/price_processor.py
"""
The core business logic orchestrator for the price update process.
"""
import logging
from datetime import datetime
from time import sleep
from typing import List, Tuple, Optional

from models.sheet_models import Payload

from clients.gamivo_client import GamivoClient, GamivoAPIError
from clients.google_sheets_client import GoogleSheetsClient
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
        """Fetches min price, max price, and stock from their respective sheet locations."""
        min_loc, max_loc, stock_loc = payload.min_price_location, payload.max_price_location, payload.stock_location

        min_price_raw = self.gsheet_client.get_data(min_loc.sheet_id, f"{min_loc.sheet_name}!{min_loc.cell}")[0][0]
        max_price_raw = self.gsheet_client.get_data(max_loc.sheet_id, f"{max_loc.sheet_name}!{max_loc.cell}")[0][0]
        stock_raw = self.gsheet_client.get_data(stock_loc.sheet_id, f"{stock_loc.sheet_name}!{stock_loc.cell}")[0][0]

        return float(min_price_raw), float(max_price_raw), int(stock_raw)

    def _find_valid_competitor(self, product_id: int, min_price: float) -> Tuple[Optional[str], Optional[float]]:
        """Finds the first competitor who is not on the blacklist."""
        offers = self.gamivo_client.get_product_offers(product_id)
        for offer in sorted(offers, key=lambda o: o.retail_price):
            if offer.seller_name not in self.blacklist:
                # Found a valid competitor, return their details
                return offer.seller_name, offer.retail_price

        # No valid competitor found (all are blacklisted or no offers)
        return None, None

    def _calculate_final_price(self, current_price, target_price, min_change, max_change, rounding, min_price,
        max_price):
        """
        Calculates the final price based on the provided logic.
        This logic is preserved from the original script to ensure business continuity.
        """
        target_price = max(target_price, min_price)
        target_price = min(target_price, max_price)

        if current_price == 0:
            return round(max(min_price, target_price - min_change), rounding)

        if current_price == target_price:
            while round(current_price, rounding) >= target_price:
                current_price -= min_change
            return round(max(min_price, current_price), rounding)

        if current_price < target_price:
            new_price = max(min_price, target_price - min_change)
            return round(new_price, rounding)

        if current_price > target_price:
            if target_price < min_price:
                return min_price

            while round(current_price - target_price, rounding) >= max_change and round(current_price - max_change,
                rounding) >= min_price:
                current_price -= max_change

            while round(current_price - target_price, rounding) >= min_change and round(current_price - min_change,
                rounding) >= min_price:
                current_price -= min_change

            return round(max(current_price, min_price), rounding)

        return round(max(current_price, min_price), rounding)

    def _process_single_payload(self, index: int, payload: Payload):
        """Processes a single payload (a single row from the sheet)."""
        try:
            min_price, max_price, stock = self._get_price_boundaries(payload)

            offer_id = self.gamivo_client.get_offer_id_by_product_id(payload.product_compare_id)
            if not offer_id:
                raise Exception(f"Offer ID not found in local DB for product {payload.product_compare_id}")

            my_offer_data = self.gamivo_client.retrieve_my_offer(offer_id)
            my_current_price_details = self.gamivo_client.calculate_seller_price(offer_id,
                my_offer_data['retail_price'])
            my_current_price = my_current_price_details.seller_price

            competitor_seller, competitor_price_raw = self._find_valid_competitor(payload.product_compare_id, min_price)

            if competitor_seller is None or competitor_price_raw is None:
                final_price = max_price
                log_msg = f"No valid competitor found. Setting price to MAX: {final_price:.2f}"
            else:
                competitor_price_details = self.gamivo_client.calculate_seller_price(offer_id, competitor_price_raw)
                competitor_price = competitor_price_details.seller_price

                final_price = self._calculate_final_price(
                    my_current_price, competitor_price,
                    payload.min_change_price, payload.max_change_price,
                    payload.rounding_precision, min_price, max_price
                )
                log_msg = (f"Success. Price={final_price:.2f}; Competing with '{competitor_seller}' ("
                           f"{competitor_price:.2f})")

            status, response = self.gamivo_client.update_offer(offer_id, my_offer_data, final_price, stock)

            if status == 200:
                self._add_log(index, log_msg, 'C')
            else:
                raise GamivoAPIError(f"Failed to update offer: {response.get('message', 'Unknown error')}", status)

        except (GamivoAPIError, Exception) as e:
            logging.error(f"Error processing '{payload.product_name}': {e}")
            self._add_log(index, f"Error: {e}", 'E')
        finally:
            self._add_log(index, datetime.now().strftime('%d/%m/%Y %H:%M:%S'), 'D')

    def _add_log(self, index: int, message: str, column: str):
        """Adds a log entry to the buffer for batch writing."""
        row_num = index + self.config.start_row + 1
        range_name = f"{self.config.main_sheet_name}!{column}{row_num}"
        self.log_buffer.append({'range': range_name, 'values': [[str(message)]]})

    def _flush_logs(self):
        """Writes all buffered logs to the Google Sheet."""
        if not self.log_buffer:
            return
        try:
            self.gsheet_client.batch_update(self.config.main_sheet_id, self.log_buffer)
            logging.info(f"Successfully wrote {len(self.log_buffer)} log entries.")
        except Exception as e:
            logging.error(f"Failed to write logs to sheet: {e}")
        finally:
            self.log_buffer = []

    def run(self):
        """The main processing loop."""
        while True:
            logging.info("--- Starting new processing cycle ---")
            try:
                sheet_data = self.gsheet_client.get_data(
                    self.config.main_sheet_id,
                    f"{self.config.main_sheet_name}!A{self.config.start_row}:W"
                )

                payloads = [Payload.from_row(row) for row in sheet_data]
                valid_payloads = [p for p in payloads if p and p.is_enabled]

                if not valid_payloads:
                    logging.info("No enabled products to process.")
                else:
                    # Fetch blacklist once using the first valid payload's config
                    self._fetch_blacklist(valid_payloads[0])

                logging.info(f"Found {len(valid_payloads)} enabled products to process.")
                for i, payload in enumerate(valid_payloads):
                    # We need the original index from the sheet_data list for logging
                    original_index = payloads.index(payload)
                    logging.info(f"Processing '{payload.product_name}'...")
                    self._process_single_payload(original_index, payload)

                self._flush_logs()

            except Exception as e:
                logging.critical(f"A critical error occurred in the main loop: {e}", exc_info=True)
                self._flush_logs()  # Attempt to write any pending logs

            logging.info(f"Cycle finished. Waiting for {self.config.loop_delay_seconds} seconds.")
            sleep(self.config.loop_delay_seconds)
