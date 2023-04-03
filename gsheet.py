import google.auth
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import pandas as pd

def check_list(lst):
    check_lst = {}
    check_lst['lol'] = all(isinstance(item, list) for item in lst)
    check_lst['lod'] = all(isinstance(item, dict) for item in lst)
    return check_lst

def list_of_lists_to_list_of_dicts(list_of_lists):
    keys = list_of_lists[0]
    list_of_dicts = []
    for values in list_of_lists[1:]:
        if len(values) < len(keys):
            values += [''] * (len(keys) - len(values))
        dict_from_list = dict(zip(keys, values))
        list_of_dicts.append(dict_from_list)
    return list_of_dicts

def list_of_dicts_to_list_of_lists(list_of_dicts):
      list_of_lists = pd.json_normalize(list_of_dicts)
      list_of_lists = [list_of_lists.columns.tolist()] + list_of_lists.values.tolist()
      return list_of_lists
    
# def list_of_dicts_to_list_of_lists(list_of_dicts):
#     keys = set()
#     for dictionary in list_of_dicts:
#         keys.update(dictionary.keys())
#     header = list(keys)
#     list_of_lists = [header]
#     for dictionary in list_of_dicts:
#         row = [dictionary.get(key, '') for key in header]
#         list_of_lists.append(row)
#     return list_of_lists

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
            data['listOfDict'] = list_of_lists_to_list_of_dicts(values)
            return data
        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

    def write_range(self, range_name: str, values_as_list: list):
        check_lst = check_list(values_as_list)
        if check_lst['lod']:
            values = list_of_dicts_to_list_of_lists(values_as_list)
        elif check_lst['lol']:
            values = values_as_list
        else:
            return "Data format error"
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
        
    def batch_read(self, sheet_ranges: list):
        data = {}
        try:
            # Build the request for batch reading multiple sheet ranges
#             batch_request = {
#                 'includeValuesInResponse': True,
#                 'ranges': sheet_ranges,
#                 'majorDimension': 'ROWS'
#             }
            # Execute the batch read request
            response = self.service.spreadsheets().values().batchGet(
                spreadsheetId=self.spreadsheet_id,
                ranges = sheet_ranges
            ).execute()

            # Iterate over each sheet range response and format the data
            for sheet_data in response.get('valueRanges', []):
                sheet_name = sheet_data['range'].split('!')[0]
                values = sheet_data.get('values', [])
                data[sheet_name] = {
                    'Range': sheet_data['range'],
                    'listOfList': values,
                    'listOfDict': list_of_lists_to_list_of_dicts(values)
                }
            return data

        except HttpError as error:
            print(f"An error occurred: {error}")
            return None

    def batch_write_ranges(self, data_dict: dict):
        updatebody = {'valueInputOption':'USER_ENTERED','data':[]}
        for range_name, values in data_dict.items():
            check_lst = check_list(values)
            if check_lst['lod']:
                values = list_of_dicts_to_list_of_lists(values)
            elif not check_lst['lol']:
                print(f"Data format error in range {range_name}")
                continue
            updatebody['data'].append({'range': range_name, 'majorDimension':'ROWS', 'values': values})
        if updatebody['data']:
            try:
                result = self.service.spreadsheets().values().batchUpdate(
                    spreadsheetId=self.spreadsheet_id, body=updatebody).execute()
                return result
            except HttpError as error:
                print(f"An error occurred: {error}")
                return None

    def batch_clear_ranges(self, range_names: list):
        data = []
        for range_name in range_names:
            data.append({'range': range_name, 'values': []})
        if data:
            try:
                result = self.service.spreadsheets().values().batchClear(
                    spreadsheetId=self.spreadsheet_id, body={'ranges': range_names}).execute()
                return result
            except HttpError as error:
                print(f"An error occurred: {error}")
                return None
