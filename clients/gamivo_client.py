# clients/gamivo_client.py
"""
A dedicated client for interacting with the Gamivo API.
This class encapsulates all API calls, data transformation, and local DB lookups.
"""
import requests
import logging
from typing import List, Dict, Any, Optional, Tuple

# Import from our new structured files
from db.sqlite_client import SQLiteClient
from models.gamivo_models import UpdateOfferPayload, OfferDetails, CalculatedPrice


class GamivoAPIError(Exception):
    """Custom exception for Gamivo API errors."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class GamivoClient:
    """Client to handle all interactions with the Gamivo API."""

    BASE_URL = "https://backend.gamivo.com/api/public/v1"

    def __init__(self, api_key: str, db_path: str = 'storage/product_offers.db'):
        """
        Initializes the GamivoClient.

        Args:
            api_key (str): The API key for authenticating with Gamivo.
            db_path (str): The file path to the local SQLite database for offer lookups.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.db_client = SQLiteClient(db_path)
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.api_key
        }

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        A helper method to make HTTP requests to the Gamivo API.
        """
        url = f"{self.BASE_URL}{endpoint}"
        try:
            response = requests.request(method, url, headers=self.headers, **kwargs)
            response.raise_for_status()  # Raises HTTPError for bad responses (4xx or 5xx)
            return response.json()
        except requests.exceptions.HTTPError as e:
            error_message = e.response.json().get("message", e.response.text)
            logging.error(f"HTTP Error {e.response.status_code} for {url}: {error_message}")
            raise GamivoAPIError(error_message, status_code=e.response.status_code) from e
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
            raise GamivoAPIError(f"Request failed: {e}") from e

    def get_offer_id_by_product_id(self, product_id: int) -> Optional[int]:
        """
        Retrieves the offer ID from the local database using a product ID.
        This method now assumes the DB connection is managed externally.
        """
        query = "SELECT id FROM product_offers WHERE product_id = ?"
        # The 'with' statement is removed to prevent open/close on each call.
        result = self.db_client.fetch_query(query, (product_id,))

        if result:
            return result[0][0]
        logging.warning(f"No offer ID found in local DB for product_id: {product_id}")
        return None

    def get_product_offers(self, product_id: int) -> List[OfferDetails]:
        """
        Gets all offers for a given product ID.
        """
        endpoint = f"/products/{product_id}/offers"
        data = self._make_request('GET', endpoint)
        return [OfferDetails(**item) for item in data]

    def retrieve_my_offer(self, offer_id: int) -> Dict[str, Any]:
        """
        Retrieves the full details of a specific offer by its ID.
        """
        endpoint = f"/offers/{offer_id}"
        return self._make_request('GET', endpoint)

    def calculate_seller_price(self, offer_id: int, retail_price: float) -> CalculatedPrice:
        """
        Calculates the seller price based on a given retail price.
        """
        endpoint = f"/offers/calculate-seller-price/{offer_id}"
        params = {'price': retail_price, 'tier_one_price': 0, 'tier_two_price': 0}
        data = self._make_request('GET', endpoint, params=params)
        return CalculatedPrice(**data)

    def update_offer(self, offer_id: int, original_offer_data: dict, new_price: float, stock: int) -> Tuple[int, dict]:
        """
        Updates an existing offer with a new price and stock.
        """
        payload_data = {
            "wholesale_mode": original_offer_data.get('wholesale_mode', 0) or 0,
            "seller_price": new_price,
            "tier_one_seller_price": original_offer_data.get('tier_one_seller_price', 0) or 0,
            "tier_two_seller_price": original_offer_data.get('tier_two_seller_price', 0) or 0,
            "keys": stock,
            "is_preorder": original_offer_data.get('is_preorder', False)
        }

        try:
            validated_payload = UpdateOfferPayload(**payload_data)
        except Exception as e:
            logging.error(f"Payload validation failed for offer {offer_id}: {e}")
            raise GamivoAPIError(f"Payload validation failed: {e}")

        endpoint = f"/offers/{offer_id}"
        json_payload = validated_payload.model_dump(exclude_none=True)

        url = f"{self.BASE_URL}{endpoint}"
        response = requests.put(url, headers=self.headers, json=json_payload)
        return response.status_code, response.json()
