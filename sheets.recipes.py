from collections import defaultdict
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from fs import Firestore
import re
import random
import boto3
import os

from botocore.exceptions import ClientError
import requests
import mimetypes

class Sheets:

    def __init__(self):
        SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        service = build('sheets', 'v4', http=http_auth)
        self.sheet = service.spreadsheets()
        self.s3_client = boto3.client('s3')

    def run(self):
        SPREADSHEET_ID = '1JZSbnesd5OGkk44GVRW4igKgwEso6oulG8S1JxFON3M'
        RANGE_NAME = 'recipes!A:L'
        result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        #header = values[0]
        header = ['id', 'brand', 'system', 'capsule', 'name', 'headline', 'label', 'capsules', 'ingredients', 'steps', 'visible', 'update']

        for r in values[1:]:
            if not r or not r[0]:
                continue

            row = {}
            data = {}

            for i, h in enumerate(header):
                data[h] = r[i] if len(r) > i and r[i] else None

            if data['update'] == False:
                continue

            #row['id'] = data['id']
            #row['type'] = 'recipe'
            #row['brand'] = data['brand']
            #row['system'] = data['system']
            row['author'] = 'captain'
            row['name'] = data['name']
            row['ingredients'] = data['ingredients'].split(';')
            row['steps'] = data['steps'].split(';')

            row['tags'] = []
            row['categories'] = []

            if data['label'] == 'iced':
                row['categories'].append('ice')
        
            row['deleted'] = False
            row['uts'] = Firestore.SERVER_TIMESTAMP

            cid = '-'.join([data['brand'], data['system'], data['id']]).lower()
            
            print(row)
            #Firestore.set2('capsules', cid, 'recipes', row)
            return

            #print(f"{data['id']} - {row['rank']}")

Sheets().run()
