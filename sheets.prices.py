from datetime import datetime
from collections import defaultdict
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from fs import Firestore
from firebase_admin.firestore import SERVER_TIMESTAMP, ArrayUnion

class Sheets:

    def __init__(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        service = build('sheets', 'v4', http=http_auth)
        self.sheet = service.spreadsheets()

    def run(self):
        SPREADSHEET_ID = '1JZSbnesd5OGkk44GVRW4igKgwEso6oulG8S1JxFON3M'
        RANGE_NAME = 'prices!A:K'
        result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])

        collection = Firestore.collection(u'capsules')
        
        header = values[0]
        for r in values[1:]:
            if not r or not r[0]:
                continue

            row = defaultdict(str)
            for i,v in enumerate(r):
                row[header[i]] = v if v else None

            row['unit'] = int(row['price_unit'].replace(',', ''))
            row['before'] = int(row['price_before'].replace(',', ''))
            row['after'] = int(row['price_after'].replace(',', ''))

            key = int(row['date'].replace('/', ''))

            row['date'] = datetime.strptime(row['date'], '%y/%m/%d')
            row['brand'] = row['brand'].lower()
            row['tags'] = row['tags'].split(',') if row['tags'] else []
            row['deleted'] = False
            row['uts'] = SERVER_TIMESTAMP
        
            docs = collection.where('brand', '==', row['brand']).stream()

            del row['id']
            del row['price_unit']
            del row['price_before']
            del row['price_after']
            del row['shipping']
            del row['brand']

            for doc in docs:
                print(f"Update {doc.id} = {key} :{row}")
                Firestore.update('capsules', doc.id, {
                    u'prices': { str(key) : row }
                })

Sheets().run()
