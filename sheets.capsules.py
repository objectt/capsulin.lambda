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
        RANGE_NAME = 'original!A:AD'
        result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        
        #header = values[0]
        header = ['update', 'id', 'brand', 'capsule', 'name', 'aroma', 'intensity', 'intensity_5', 'bitterness', 'acidity', 'body', 'roastiness', 'roastlevel', 'beans', 'weight', 'content', 'sizes', 'decaf', 'ice', 'latte', 'nocaf', 'limited', 'unavailable', 'new', 'price', 'coo', 'headline', 'url', 'image', 'comment' ]

        for r in values[1:]:
            if not r or not r[0]:
                continue

            row = {}
            data = {}

            for i, h in enumerate(header):
                data[h] = r[i] if len(r) > i and r[i] else None

            #if data['capsule'] != 'vertuo':
            #if not data['comment']:
            #if data['brand'] != 'droptop':
            if data['update'] != 'Y':
                continue

            row['id'] = data['id']
            row['type'] = 'capsule'
            row['brand'] = data['brand']
            row['system'] = data['capsule']
            row['name'] = data['name']

            row['flavor'] = {
                'bitterness': data['bitterness'],
                'acidity': data['acidity'],
                'body': data['body'],
                'intensity': data['intensity_5'],
                'roastiness': data['roastiness'],
                'roastlevel': data['roastlevel'],
                'aroma': data['aroma'].split(',') if data['aroma'] else []
            }

            row['beans'] = data['beans'].split(',') if 'beans' in data and data['beans'] else []
            row['beans'] = [ e.strip() for e in row['beans'] ]
            
            row['sizes'] = {}
            sizes = data['sizes'].strip().split(',') if 'sizes' in data and data['sizes'] else []
            
            for c in sizes:
                match = re.search(r'^(?P<s>[\w\s]+[^\(\)])(?:\((?P<n>[\w.]+)\))?$', c)
                if match:
                    row['sizes'][match.group('s').strip()] = int(match.group('n').replace('ml', '')) if match.group('n') else 0

            row['content'] = {}
            content = data['content'].split(',') if 'content' in data and data['content'] else []

            for c in content:
                match = re.search(r'^(?P<s>\w+[^\(\)])(?:\((?P<n>\d+)\))?$', c)
                if match:
                    row['content'][match.group('s')] = int(match.group('n')) if match.group('n') else 0

            #row['ice'] = data['ice'] == 'O'
            row['limited'] = data['limited'] == 'O'
            row['new'] = data['new'] == 'O'
            row['decaf'] = data['decaf'] == 'O'
            row['nocaf'] = data['nocaf'] == 'O'

            row['tags'] = []
            row['categories'] = []

            # TODO for new entry only
            row['stat'] = {}
            row['prices'] = {} 

            if row['decaf']:
                row['categories'].append('decaf')
            if row['nocaf']:
                row['categories'].append('nocaf')

            if row['new']:
                row['tags'].append('new')
            if row['limited']:
                row['tags'].append('limited')

            row['price'] = {}
            row['price']['unit'] = data['price']

            row['weight'] = data['weight']
            row['coo'] = data['coo']
            row['url'] = data['url']
            row['images'] = [ data['image'] ] if data['image'] else []
            row['images'] = [ img.split('?')[0] for img in row['images'] ]

            row['available'] = data['unavailable'] != 'O'
            row['headline'] = data['comment']
        
            row['deleted'] = False
            row['uts'] = Firestore.SERVER_TIMESTAMP

            if not row['available']:
                Firestore.delete('capsules', data['id'])
                print(f"delete {data['id']}")
                continue

            #row['data'] = Firestore.DELETE_FIELD

            row['rank'] = random.randint(1, 50)
            data['id'] = '-'.join([row['brand'], row['system'], row['id']]).lower()
            
            if row['images']:
                r = self.upload_image(data['id'], row['images'][0])
                if r:
                    row['images'][0] = f'https://img.capsulin.coffee/thumbnails/{r}'
            
            Firestore.set('capsules', data['id'], row)

            print(f"{data['id']} - {row['rank']}")

    def upload_image(self, key, url):
        bucket = 'img.capsulin.coffee'

        try:
            headers = { 
                #'Referer': 'https://www.dolce-gusto.co.kr',
                'Referer': 'https://www.nespresso.com', 
                #'Referer': 'https://smartstore.naver.com',
                #'Referer': 'https://shop.illycaffe.co.k',
                #'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*',
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
            }

            img = requests.get(url, headers=headers, stream=True, timeout=2).raw
            extension = url.split('.')[-1]
            extension = extension if extension == 'png' else 'jpeg'
            #content_type = img.headers['content-type']
            #extension = mimetypes.guess_extension(content_type)
            #filename = key + extension
            filename = f'{key}.{extension}'
            print(f'uploading file {filename} to s3')
    
            self.s3_client.upload_fileobj(img, bucket, f'thumbnails/{filename}', ExtraArgs={ 'ContentType': f'image/{extension}' })
            return filename
        #except requests.exceptions.RequestException as e:
        except Exception as e:
            print(f' {key} {e}')
            return None

Sheets().run()
