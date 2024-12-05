from collections import defaultdict
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

import os
import json
from fs import Firestore

class Sheets:

    def __init__(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        self.service = build('sheets', 'v4', http=http_auth)

    def run(self):
        with open('./recipes.json', 'r') as f:
            data = json.load(f)

        body = {
            #'valueInputOption': 'USER_ENTERED',
            'values': []
        }
            
        headers = ['brand', 'system', 'name', 'headline', 'label', 'capsules', 'ingredients', 'steps', 'visible']
        body['values'].append(headers)

        for e in data:
            row = {
                'brand': 'nespresso',
                'system': e['cate'].lower(),
                'name': e['recipe_name_ko'],
                'headline': e['pop']['desc'],
                'label': e['pop']['label'].lower(),
                'capsules': ';'.join(e['pop']['info1'].split(',')),
                'ingredients': ';'.join([ s.strip() for s in e['pop']['info2'].split(',') ]),
                'steps': ';'.join(e['pop']['order']),
                'visible': e['visible']
            }

            body['values'].append(list(row.values()))

        SPREADSHEET_ID = '1JZSbnesd5OGkk44GVRW4igKgwEso6oulG8S1JxFON3M'
        self.service.spreadsheets().values().append(
            spreadsheetId=SPREADSHEET_ID, 
            range='recipes!A1',
            valueInputOption='USER_ENTERED',
            body=body
        ).execute()

    def upload(self):
        row['uts'] = Firestore.SERVER_TIMESTAMP

Sheets().run()
