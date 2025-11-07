import asyncio
import logging
import random
import ssl
from typing import List, Dict, Any, Optional, Tuple

import httpx
from httpx import Limits
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from db.sqlite_client import SQLiteClient
from models.gamivo_models import UpdateOfferPayload, OfferDetails, CalculatedPrice


class GamivoAPIError(Exception):
    """Custom exception for Gamivo API errors."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


class RetryableAPIError(Exception):
    """Custom exception for server-side errors (5xx) that should trigger a retry."""
    pass


# --- LOGIC RETRY ---
# Áp dụng cho các phương thức gọi API
# Sẽ thử lại 5 lần, thời gian chờ tăng theo hàm mũ (2s, 4s, 8s, 16s)
RETRY_DECORATOR = retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    # THÊM ssl.SSLError và httpx.ReadError vào danh sách retry
    retry=retry_if_exception_type((
        RetryableAPIError,
        httpx.RequestError,
        ssl.SSLError,
        httpx.ReadError
    ))
)


class GamivoClient:
    """Client to handle all interactions with the Gamivo API using asynchronous calls."""

    BASE_URL = "https://backend.gamivo.com/api/public/v1"

    def __init__(self, api_key: str, db_path: str = 'storage/product_offers.db'):
        """
        Initializes the AsyncGamivoClient and sets up the shared HTTP client.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")

        self.api_key = api_key

        # Khởi tạo DB client và load cache một lần
        self.db_client = SQLiteClient(db_path)
        self.db_client.load_product_offer_map()

        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.api_key
        }

        connection_limits = Limits(max_keepalive_connections=2, max_connections=2)

        # Khởi tạo AsyncClient để tái sử dụng kết nối
        self._http_client = httpx.AsyncClient(
            base_url=self.BASE_URL,
            headers=self.headers,
            timeout=15.0,
            limits=connection_limits  # Áp dụng giới hạn vào client
        )

    # Context Manager cho việc quản lý AsyncClient
    async def __aenter__(self):
        """Enter the asynchronous context."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Exit the asynchronous context and close the HTTP client."""
        await self._http_client.aclose()

    @RETRY_DECORATOR
    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        A helper method to make ASYNCHRONOUS HTTP requests to the Gamivo API with retry logic.
        """
        url = endpoint  # Đã cấu hình base_url trong AsyncClient

        try:
            response = await self._http_client.request(method, url, **kwargs)

            # KIỂM TRA LỖI CÓ THỂ RETRY (5xx Server Errors)
            if 500 <= response.status_code < 600:
                # Raise một lỗi tùy chỉnh để kích hoạt tenacity retry
                logging.warning(f"Server returned retryable error {response.status_code} for {url}. Retrying...")
                raise RetryableAPIError(f"Server error: {response.status_code}")

            # Raises HTTPStatusError cho các lỗi 4xx (lỗi không retry)
            response.raise_for_status()
            return response.json()

        except httpx.HTTPStatusError as e:
            # Xử lý các lỗi 4xx (ví dụ: 401, 404, 429)
            error_message = e.response.json().get("message", e.response.text)
            logging.error(f"HTTP Error {e.response.status_code} for {url}: {error_message}")
            raise GamivoAPIError(error_message, status_code=e.response.status_code) from e

        except httpx.RequestError as e:
            # Lỗi kết nối, timeout. Tenacity sẽ bắt lỗi này để retry.
            logging.error(f"Connection Error for {url}: {e}. Retrying...")
            raise e  # Raise lại lỗi để Tenacity bắt và retry

        except ssl.SSLError as e:
            logging.error(f"SSL Error for {endpoint}: {e}. Retrying...")
            raise e  # Tenacity sẽ bắt lỗi này

        except Exception as e:
            # Các lỗi khác (ví dụ: JSON parsing)
            logging.error(f"An unexpected error occurred for {url}: {e}")
            raise GamivoAPIError(f"Unexpected error: {e}") from e

    # --- Phương thức sử dụng Cache (Đồng bộ) ---

    def get_offer_id_by_product_id(self, product_id: int) -> Optional[int]:
        """
        Retrieves the offer ID from the local cache map (quick and thread-safe).
        """
        offer_id = self.db_client.product_offer_map.get(product_id)

        if offer_id is not None:
            return offer_id

        logging.warning(f"No offer ID found in cache for product_id: {product_id}")
        return None

    # --- Các phương thức API (Bất đồng bộ) ---
    # Tất cả các phương thức gọi API đều sử dụng _make_request đã tích hợp retry

    async def get_product_offers(self, product_id: int) -> List[OfferDetails]:
        """
        Gets all offers for a given product ID asynchronously with retry.
        """
        endpoint = f"/products/{product_id}/offers"
        data = await self._make_request('GET', endpoint)
        return [OfferDetails(**item) for item in data]

    async def retrieve_my_offer(self, offer_id: int) -> Dict[str, Any]:
        """
        Retrieves the full details of a specific offer by its ID asynchronously with retry.
        """
        endpoint = f"/offers/{offer_id}"
        return await self._make_request('GET', endpoint)

    async def calculate_seller_price(self, offer_id: int, retail_price: float) -> CalculatedPrice:
        """
        Calculates the seller price based on a given retail price asynchronously with retry.
        """
        endpoint = f"/offers/calculate-seller-price/{offer_id}"
        params = {'price': retail_price, 'tier_one_price': 0, 'tier_two_price': 0}
        data = await self._make_request('GET', endpoint, params=params)
        return CalculatedPrice(**data)

    async def update_offer(self, offer_id: int, original_offer_data: dict, new_price: float, stock: int) -> Tuple[
        int, dict]:
        """
        Updates an existing offer with a new price and stock asynchronously with retry.
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

        # --- ĐÃ XÓA DÒNG GỌI _make_request BỊ LỖI ---

        # --- Phiên bản update_offer TỰ TÍCH HỢP RETRY ---
        @RETRY_DECORATOR
        async def _internal_update():
            response = await self._http_client.put(endpoint, json=json_payload)

            if 500 <= response.status_code < 600:
                logging.warning(f"Server returned retryable error {response.status_code} for {endpoint}. Retrying...")
                raise RetryableAPIError(f"Server error: {response.status_code}")

            response.raise_for_status()
            return response.status_code, response.json()

        try:
            # Chỉ gọi _internal_update MỘT LẦN
            return await _internal_update()
        except httpx.HTTPStatusError as e:
            error_message = e.response.json().get("message", e.response.text)
            raise GamivoAPIError(error_message, status_code=e.response.status_code) from e
        except Exception as e:
            raise GamivoAPIError(f"Update failed after retries: {e}")
