from typing import Optional

from pydantic import BaseModel, Field


class UpdateOfferPayload(BaseModel):
    """
    Pydantic model for validating the payload sent to the 'edit offer' endpoint.
    The validation rules from the old dataclass are now handled by Field constraints.
    """
    wholesale_mode: int = Field(..., description="Wholesale mode for the offer.", ge=0, le=2)
    seller_price: Optional[float] = Field(None, description="The main price for the seller.", ge=0)
    tier_one_seller_price: Optional[float] = Field(0, description="Tier 1 price.", ge=0)
    tier_two_seller_price: Optional[float] = Field(0, description="Tier 2 price.", ge=0)
    status: int = Field(1, description="Offer status (1 for active, 0 for inactive).", ge=0, le=1)
    keys: Optional[int] = Field(None, description="Number of keys in stock.", ge=0, le=10000)
    is_preorder: Optional[bool] = Field(False, description="Indicates if the offer is a pre-order.")

    class Config:
        """Pydantic model configuration."""
        anystr_strip_whitespace = True
        validate_assignment = True


class OfferDetails(BaseModel):
    """
    Represents the detailed structure of a single offer retrieved from Gamivo.
    """
    seller_name: str
    retail_price: float
    # Add other fields from the API response as needed for type safety
    # e.g., id: int, product_id: int, etc.


class CalculatedPrice(BaseModel):
    """
    Represents the response from the price calculation endpoint.
    """
    seller_price: float
    # Add other fields like 'retail_price', 'fee', etc. if they exist in the response
