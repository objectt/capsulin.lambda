from collections import defaultdict
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
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
        RANGE_NAME = 'machines!A:U'
        result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])

        # capacity, heatuptime
        header = ['update', 'id', 'brand', 'system', 'name', 'pressure', 'weight', 'dimensions', 'watertank', 'milktank', 'capsules', 'colors', 'sizes', 'modes', 'price', 'headline', 'url', 'image', 'coo', 'dt', 'etc']
        for r in values[1:]:
            if not r or not r[0]:
                continue

            data = defaultdict(str)
            row = defaultdict(str)

            for i,v in enumerate(r):
                row[header[i]] = v if v else None

            if row['update'] != 'Y':
                continue
            
            _id = '-'.join([row['brand'], row['id']]).lower()
            data['brand'] = row['brand']
            data['system'] = row['system']
            data['name'] = row['name']

            data['pressure'] = row['pressure']
            data['weight'] = row['weight']
            data['dimensions'] = row['dimensions']
            data['water'] = row['watertank']
            data['milk'] = row['milktank']
            data['capsules'] = row['capsules']
            data['colors'] = row['colors'] #.split(',')
            data['sizes'] = row['sizes'].split(',') if row['sizes'] else []
            data['modes'] = row['modes'] #.split(',')
            data['headline'] = row['headline']
            data['url'] = row['url']
            data['coo'] = row['coo']
            data['dt'] = row['dt']
            data['etc'] = row['etc']

            data['images'] = [] 
            data['tags'] = [] #row['tags'].split(',') if row['tags'] else []

            data['available'] = True
            data['deleted'] = False
            data['uts'] = Firestore.SERVER_TIMESTAMP

            data['prices'] = {}
            data['price'] = {
                'unit': row['price']
            }

            if row['image']:
                row['image'] = row['image'].split('?')[0]
                r = self.upload_image(_id, row['image'])
                if r:
                    data['images'].append(f'https://img.capsulin.coffee/thumbnails/{r}')
            
            data['rank'] = random.randint(1, 50)
 
            print(f"{_id} - {data['rank']}")
            Firestore.insert('machines', _id, data)

    def upload_image(self, key, url):
        bucket = 'img.capsulin.coffee'

        try:
            headers = { 
                #'Referer': 'https://shop.illycaffe.co.kr',
                'Referer': 'https://www.nespresso.com',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            }

            img = requests.get(url, headers=headers, stream=True, timeout=2).raw
            extension = url.split('.')[-1]
            extension = extension if extension == 'png' else 'jpeg'
            filename = f'{key}.{extension}'
            print(f'uploading file {filename} to s3')
    
            self.s3_client.upload_fileobj(img, bucket, f'thumbnails/{filename}', ExtraArgs={ 'ContentType': f'image/{extension}' })
            return filename
        except Exception as e:
            print(e)
            return None

Sheets().run()
