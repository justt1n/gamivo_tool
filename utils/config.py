import os
from dotenv import load_dotenv


class Config:
    def __init__(self, env_file='settings.env'):
        load_dotenv(env_file)
        # Google Sheets
        self.google_key_path = os.getenv('GOOGLE_KEY_PATH')
        self.main_sheet_id = os.getenv('MAIN_SHEET_ID')
        self.main_sheet_name = os.getenv('MAIN_SHEET_NAME')
        self.start_row = int(os.getenv('START_ROW', 2))
        # Gamivo
        self.gamivo_api_key = os.getenv('GAMIVO_API_KEY')

        # Database
        self.db_path = os.getenv('DB_PATH', 'storage/product_offers.db')
        # App Logic
        self.retries_time = int(os.getenv('RETRIES_TIME', 3))
        self.retries_time_sleep = int(os.getenv('RETRIES_TIME_SLEEP', 3))
        self.loop_delay_seconds = int(os.getenv('LOOP_DELAY_SECONDS', 300))
