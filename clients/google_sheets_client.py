from google.oauth2 import service_account
from googleapiclient.discovery import build


class GoogleSheetsClient:
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def __init__(self, key_path):
        creds = service_account.Credentials.from_service_account_file(key_path, scopes=self.SCOPES)
        self.service = build('sheets', 'v4', credentials=creds)

    def get_data(self, spreadsheet_id, range_name):
        result = self.service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id, range=range_name
        ).execute()
        return result.get('values', [])

    def batch_update(self, spreadsheet_id, data):
        body = {'data': data, 'valueInputOption': 'RAW'}
        self.service.spreadsheets().values().batchUpdate(
            spreadsheetId=spreadsheet_id, body=body
        ).execute()

    def batch_get(self, spreadsheet_id, ranges):
        """
        Fetches multiple ranges from a spreadsheet at once.
        """
        result = self.service.spreadsheets().values().batchGet(
            spreadsheetId=spreadsheet_id, ranges=ranges
        ).execute()
        return result.get('valueRanges', [])
