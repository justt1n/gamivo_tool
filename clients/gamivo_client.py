"""
A dedicated client for interacting with the Gamivo API.
This class encapsulates all API calls, data transformation, and local DB lookups.
VERSION: ASYNCHRONOUS + RETRY (Quiet)
"""
import logging
import asyncio
from typing import List, Dict, Any, Optional, Tuple

import httpx
# Thêm Tenacity
from tenacity import AsyncRetrying, wait_exponential, stop_after_attempt, retry_if_exception, RetryError

# Import from our new structured files
from db.sqlite_client import SQLiteClient
from models.gamivo_models import UpdateOfferPayload, OfferDetails, CalculatedPrice


class GamivoAPIError(Exception):
    """Custom exception for Gamivo API errors."""

    def __init__(self, message, status_code=None):
        super().__init__(message)
        self.status_code = status_code


# --- HÀM HELPER MỚI CHO TENACITY ---
def _is_retryable_exception(exception: BaseException) -> bool:
    """Chỉ retry nếu là lỗi mạng, 5xx, hoặc 429."""
    if isinstance(exception, (httpx.RequestError, asyncio.TimeoutError)):
        # Lỗi mạng, connection error, timeout, etc.
        return True

    if isinstance(exception, GamivoAPIError):
        # Lỗi server 5xx hoặc Rate Limit 429
        if exception.status_code in {429, 500, 502, 503, 504}:
            # SỬA: Đổi sang logging.DEBUG
            logging.debug(f"Retryable API Error {exception.status_code}. Will retry.")
            return True

    if isinstance(exception, httpx.HTTPStatusError):
        # Lỗi server 5xx hoặc Rate Limit 429
        if exception.response.status_code in {429, 500, 502, 503, 504}:
            # SỬA: Đổi sang logging.DEBUG
            logging.debug(f"Retryable HTTP Error {exception.response.status_code}. Will retry.")
            return True

    # Không retry các lỗi khác (ví dụ: 400, 401, 403, 404, lỗi logic)
    return False
# --- KẾT THÚC HÀM HELPER ---


class GamivoClient:
    """Client to handle all interactions with the Gamivo API."""

    BASE_URL = "https://backend.gamivo.com/api/public/v1"

    def __init__(self, api_key: str, db_path: str, client: httpx.AsyncClient, retries: int = 3, retry_sleep_max: int = 30):
        """
        Initializes the GamivoClient.

        Args:
            api_key (str): The API key for authenticating with Gamivo.
            db_path (str): The file path to the local SQLite database.
            client (httpx.AsyncClient): An active async HTTP client instance.
            retries (int): Số lần thử lại tối đa.
            retry_sleep_max (int): Thời gian chờ tối đa (giây) giữa các lần thử.
        """
        if not api_key:
            raise ValueError("API key cannot be empty.")
        self.api_key = api_key
        self.db_client = SQLiteClient(db_path) # Chỉ khởi tạo object
        self._client = client  # Lưu trữ HTTP client
        self.headers = {
            "accept": "application/json",
            "Content-Type": "application/json",
            "Authorization": self.api_key
        }
        # Cấu hình retry (Bắt đầu 1s, max 10s, thử 4 lần)
        self.retryer = AsyncRetrying(
            wait=wait_exponential(multiplier=1, min=1, max=10), # Tăng tốc retry
            stop=stop_after_attempt(4), # Tăng 1 lần thử
            retry=retry_if_exception(_is_retryable_exception),
            reraise=True # Ném ra lỗi gốc sau khi đã hết số lần thử
        )


    async def _make_request(self, method: str, endpoint: str, **kwargs) -> Any:
        """
        A helper method to make async HTTP requests to the Gamivo API.
        """
        url = f"{self.BASE_URL}{endpoint}"

        # Bọc hàm request trong một hàm async nội bộ để retryer có thể gọi nó
        async def _request_internal():
            try:
                # Dùng httpx client bất đồng bộ
                response = await self._client.request(method, url, headers=self.headers, **kwargs)
                response.raise_for_status()  # Vẫn hoạt động
                return response.json()
            except httpx.HTTPStatusError as e:
                try:
                    error_message = e.response.json().get("message", e.response.text)
                except Exception:
                    error_message = e.response.text

                # SỬA: Phân loại log
                if not _is_retryable_exception(e):
                    logging.error(f"HTTP Error {e.response.status_code} for {url}: {error_message}")
                else:
                    logging.debug(f"Retryable HTTP Error {e.response.status_code} for {url}: {error_message}")

                raise GamivoAPIError(error_message, status_code=e.response.status_code) from e

            except httpx.RequestError as e:
                # SỬA: Đổi sang logging.DEBUG (vì đây là lỗi [SSL])
                logging.debug(f"Request failed (will retry): {e}")
                raise GamivoAPIError(f"Request failed: {e}") from e

        # Gọi hàm nội bộ thông qua retryer
        try:
            return await self.retryer(_request_internal)
        except RetryError as e:
            # SỬA LỖI: Dùng .retry_state.attempt_number và .outcome.exception()
            logging.critical(f"Request to {endpoint} failed after {e.retry_state.attempt_number} attempts.")
            raise e.retry_state.outcome.exception() from e # Ném ra lỗi cuối cùng

    async def get_offer_id_by_product_id(self, product_id: int) -> Optional[int]:
        """
        Retrieves the offer ID from the local database using a product ID.
        This now runs the blocking DB call in a separate thread.
        """
        query = "SELECT id FROM product_offers WHERE product_id = ?"

        # Dùng asyncio.to_thread để chạy tác vụ blocking (DB)
        # Hàm fetch_query (mới) sẽ tự mở/đóng kết nối
        result = await asyncio.to_thread(
            self.db_client.fetch_query,
            query,
            (product_id,)
        )

        if result:
            return result[0][0]
        logging.warning(f"No offer ID found in local DB for product_id: {product_id}")
        return None

    async def get_product_offers(self, product_id: int) -> List[OfferDetails]:
        """
        Gets all offers for a given product ID. (Async)
        """
        endpoint = f"/products/{product_id}/offers"
        data = await self._make_request('GET', endpoint)
        return [OfferDetails(**item) for item in data]

    async def retrieve_my_offer(self, offer_id: int) -> Dict[str, Any]:
        """
        Retrieves the full details of a specific offer by its ID. (Async)
        """
        endpoint = f"/offers/{offer_id}"
        return await self._make_request('GET', endpoint)

    async def calculate_seller_price(self, offer_id: int, retail_price: float) -> CalculatedPrice:
        """
        Calculates the seller price based on a given retail price. (Async)
        """
        endpoint = f"/offers/calculate-seller-price/{offer_id}"
        params = {'price': retail_price, 'tier_one_price': 0, 'tier_two_price': 0}
        data = await self._make_request('GET', endpoint, params=params)
        return CalculatedPrice(**data)

    async def update_offer(self, offer_id: int, original_offer_data: dict, new_price: float, stock: int) -> Tuple[int, dict]:
        """
        Updates an existing offer with a new price and stock. (Async + Retry)
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

        # Bọc hàm request trong một hàm async nội bộ
        async def _update_internal():
            try:
                # Dùng httpx client bất đồng bộ (thay vì requests.put)
                response = await self._client.put(url, headers=self.headers, json=json_payload)

                # Chỉ raise (để retry) nếu là 429 hoặc 5xx
                if response.status_code in {429, 500, 502, 503, 504}:
                    response.raise_for_status()

                # Nếu là 2xx hoặc 400, 403 (không retry), trả về bình thường
                return response.status_code, response.json()
            except httpx.HTTPStatusError as e:
                # SỬA: Đổi sang logging.DEBUG
                logging.debug(f"Retrying update_offer due to {e.response.status_code}")
                raise GamivoAPIError(e.response.text, status_code=e.response.status_code) from e
            except httpx.RequestError as e:
                # SỬA: Đổi sang logging.DEBUG
                logging.debug(f"Retrying update_offer due to network error: {e}")
                raise GamivoAPIError(f"Request failed: {e}") from e

        try:
            # Gọi hàm nội bộ thông qua retryer
            return await self.retryer(_update_internal)
        except RetryError as e:
            # SỬA LỖI: Dùng .retry_state.attempt_number và .outcome.exception()
            logging.error(f"Update offer {offer_id} failed after many attempts.")
            last_exc = e.last_attempt.outcome.exception()
            if isinstance(last_exc, GamivoAPIError):
                return last_exc.status_code or 500, {"message": str(last_exc)}
            return 500, {"message": str(last_exc)}
        except Exception as e:
             if isinstance(e, GamivoAPIError):
                 return e.status_code, {"message": str(e)}
             logging.error(f"Non-retryable error during update_offer: {e}")
             return 500, {"message": f"Non-retryable error: {e}"}
