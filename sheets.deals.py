from collections import defaultdict
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import datetime
import os

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

class Sheets:

    def __init__(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        service = build('sheets', 'v4', http=http_auth)
        self.sheet = service.spreadsheets()
        
        curPath = os.path.dirname(os.path.abspath(__file__))
        keyFile = curPath + "/serviceAccountKey.json"
        
        if (not len(firebase_admin._apps)):
            cred = credentials.Certificate(keyFile)
            firebase_admin.initialize_app(cred)
        
        self.fs = firestore.client()

    def run(self):
        SPREADSHEET_ID = '1UIBCBfMJ0aRu8IjmhAnpmIaxAmn8PPisGJwhD1ZugyA'
        RANGE_NAME = 'deals!A:O'
        result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        header = ['processed', 'collection', 'id', 'brand', 'system', 'quantity', 'price_before_total', 'discount', 'price_total', 'price_unit', 'url', 'seller', 'official', 'oversea', 'dt']

        for r in values[1:]:
            if not r or not r[0]:
                continue

            row = {}
            data = {}

            for i, h in enumerate(header):
                data[h] = r[i] if len(r) > i and r[i] else None

            if data['processed'] == 'Y':
                continue

            row['url'] = data['url']
            row['price_unit'] = data['price_unit']
            row['price_before'] = data['price_total']
            row['price_discount'] = data['discount']
            row['price_after'] = data['price_total']
            row['quantity'] = data['quantity']
            row['seller'] = data['seller']
            row['uts'] = firestore.SERVER_TIMESTAMP

            dt = datetime.datetime.now().strftime('%Y/%m/%d') if not data['dt'] else datetime.datetime.strptime(data['dt'], '%m/%d/%Y').strftime('%Y/%m/%d')
            collection = data['collection']
            doc = data['id']

            if doc:
                data = self.fs.collection(collection).document(doc).get().to_dict()
                row['price'] = {
                    'unit': data['price']['unit'] if 'unit' in data['price'] else data['price'],
                    'min': min([ data['price']['min'] if 'min' in data['price'] and data['price']['min'] else data['price']['unit'], row['price_unit'] ]),
                    'max': max([ data['price']['max'] if 'max' in data['price'] and data['price']['max'] else data['price']['unit'], row['price_unit'] ])
                }

                self.fs.collection(collection).document(doc).update({ "uts": row['uts'], "price": row['price'],  f"prices.{dt}": row['price_unit'] })
                #self.fs.collection(collection).document(doc).collection('deals').add(row)
                print(row)
            else:
                docs = self.fs.collection(collection) \
                    .where(filter=firestore.FieldFilter("brand", "==", data['brand'])) \
                    .where(filter=firestore.FieldFilter("system", "==", data['system'])) \
                    .stream()

                for doc in docs:
                    data = doc.to_dict()
                    row['price'] = {
                        'unit': data['price']['unit'],
                        'min': min([ data['price']['min'] if 'min' in data['price'] and data['price']['min'] else data['price']['unit'], row['price_unit'] ]),
                        'max': max([ data['price']['max'] if 'max' in data['price'] and data['price']['max'] else data['price']['unit'], row['price_unit'] ]),
                    }

                    self.fs.collection(collection).document(doc.id).update({ "uts": row['uts'], "price": row['price'],  f"prices.{dt}": row['price_unit'] })
                    #self.fs.collection(collection).document(doc.id).collection('deals').add(row)
                    print(doc.id)

Sheets().run()
