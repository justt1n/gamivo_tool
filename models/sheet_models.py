import logging
from typing import List, Optional

from pydantic import BaseModel, Field, ValidationError


class SheetLocation(BaseModel):
    """Represents a specific location (cell) in a Google Sheet."""
    sheet_id: str
    sheet_name: str
    cell: str


class Payload(BaseModel):
    """
    Represents a single row of configuration data from the main Google Sheet.
    Validates and type-coerces data upon creation.
    """
    # This field is for internal tracking, not from the sheet data itself.
    sheet_row_num: int = Field(..., description="The actual row number in the Google Sheet for logging.")

    # Metadata and Control
    is_enabled_str: str = Field(..., alias='0')
    product_name: str = Field(..., alias='1')
    note: Optional[str] = Field(None, alias='2')
    last_update: Optional[str] = Field(None, alias='3')
    product_link: Optional[str] = Field(None, alias='4')

    # Core Logic Parameters
    product_compare_id: int = Field(..., alias='5')
    min_change_price: float = Field(0.0, alias='6', ge=0)
    max_change_price: float = Field(0.0, alias='7', ge=0)
    rounding_precision: int = Field(2, alias='8', ge=0)

    # Sheet Locations for Price/Stock
    min_price_location: SheetLocation
    max_price_location: SheetLocation
    stock_location: SheetLocation
    blacklist_location: SheetLocation
    mode: Optional[str] = Field(None, alias='27') #AB

    # Fields not from the sheet, but useful for context
    price: Optional[str] = Field(None, alias='21')
    seller: Optional[str] = Field(None, alias='22')
    current_price: Optional[float] = Field(None, alias='28')
    target_price: Optional[float] = Field(None, alias='29')

    class Config:
        """Allow population by field name (alias)."""
        populate_by_name = True


    @property
    def get_mode(self):
        return int(self.mode) if self.mode is not None else 1

    @property
    def is_enabled(self) -> bool:
        """Returns True if the 'CHECK' column is '1'."""
        return self.is_enabled_str == '1'

    @classmethod
    def from_row(cls, row_data: List[str], sheet_row_num: int) -> Optional['Payload']:
        """
        Factory method to create a Payload instance from a list (a sheet row).
        Handles mapping list indices to model fields and validation.

        Args:
            row_data (List[str]): The list of cell values for the row.
            sheet_row_num (int): The actual 1-based row number from the Google Sheet.
        """
        if not row_data or not row_data[0]:
            return None  # Skip empty rows

        # Pad the row with None values to prevent IndexError
        padded_row = (row_data + [None] * 30)[:30]

        try:
            # Map list indices to a dictionary for Pydantic
            data_dict = {str(i): val for i, val in enumerate(padded_row)}

            # Manually add the sheet_row_num
            data_dict['sheet_row_num'] = sheet_row_num

            # Manually construct nested models
            data_dict['min_price_location'] = {'sheet_id': padded_row[9], 'sheet_name': padded_row[10],
                                               'cell': padded_row[11]}
            data_dict['max_price_location'] = {'sheet_id': padded_row[12], 'sheet_name': padded_row[13],
                                               'cell': padded_row[14]}
            data_dict['stock_location'] = {'sheet_id': padded_row[15], 'sheet_name': padded_row[16],
                                           'cell': padded_row[17]}
            data_dict['blacklist_location'] = {'sheet_id': padded_row[18], 'sheet_name': padded_row[19],
                                               'cell': padded_row[20]}

            return cls(**data_dict)
        except ValidationError as e:
            product_name = padded_row[1] or f"Row {sheet_row_num}"
            logging.warning(f"Skipping row for '{product_name}' due to validation error: {e}")
            return None
        except (TypeError, ValueError) as e:
            product_name = padded_row[1] or f"Row {sheet_row_num}"
            logging.warning(f"Skipping row for '{product_name}' due to type conversion error: {e}")
            return None
