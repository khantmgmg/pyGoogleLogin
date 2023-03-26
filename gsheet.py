import google.auth
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
class GoogleSheets:
    def __init__(self, service_account_info: dict, spreadsheet_id: str):
        self.spreadsheet_id = spreadsheet_id
        self.credentials = self.get_credentials(service_account_info)
        self.service = build('sheets', 'v4', credentials=self.credentials)

    def get_credentials(self, service_account_info: dict):
        scopes = ['https://www.googleapis.com/auth/spreadsheets']
        return Credentials.from_service_account_info(service_account_info, scopes=scopes)

    def read_range(self, range_name: str):
        data = {}
        try:
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id, range=range_name).execute()
            values = result.get('values', [])
            data['listOfList'] = values
            data['listOfDict'] = []
            keys = values[0]
            for value in values[1:]:
                if len(keys) > len(value):
                    for i in range(len(keys) - len(value)):
                        value += ['']
                data['listOfDict'].append(dict(zip(keys, value)))
            return data
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

    def write_range(self, range_name: str, values: list):
        body = {'values': values}
        try:
            result = self.service.spreadsheets().values().update(
                spreadsheetId=self.spreadsheet_id, range=range_name,
                valueInputOption='RAW', body=body).execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

    def clear_range(self, range_name: str):
        try:
            body = {}
            result = self.service.spreadsheets().values().clear(
                spreadsheetId=self.spreadsheet_id, range=range_name, body=body).execute()
            return result
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None
