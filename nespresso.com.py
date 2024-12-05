# -*- coding: UTF-8 -*-

import json
import re
from datetime import datetime
from collections import defaultdict
from pymongo import MongoClient, errors

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import logging
from fs import Firestore

from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

class Nespresso(scrapy.Spider):
    name = 'Nespresso'

    def __init__(self, *args, **kwargs):
        try:
            client = MongoClient('mongodb://127.0.0.1:27017/?retryWrites=true&w=majority')
            self.db = client.coffee
        except errors.ConnectionFailure as e:
            print(e)
            sys.exit(0)
        except errors.ServerSelectionTimeoutError as e:
            print(e)
            sys.exit(0)
        
        scrapy.Spider.__init__(self)

        self.collection = self.db.capsules
        #self.logger.propagate = False
        #self.logger.setLevel(logging.WARNING)
        self.concurrent_requests = 1
        #self.download_delay = 0.1

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        service = build('sheets', 'v4', credentials=creds)
        self.sheet = service.spreadsheets()
        
        #result = self.sheet.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        #values = result.get('values', [])

    def append_row(self, values):
        SPREADSHEET_ID = '1JZSbnesd5OGkk44GVRW4igKgwEso6oulG8S1JxFON3M'
        RANGE_NAME = 'crawler!A2'

        body = { 'values': values }
        self.sheet.values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', body=body).execute()

    def start_requests(self):
        urls = [
            'https://www.nespresso.com/kr/ko/order/capsules/vertuo',
            #'https://www.nespresso.com/kr/ko/order/capsules/original'
        ]

        headers = {
            "Connection": "keep-alive",
        }

        for url in urls:
            capsule = url.split('/')[-1]
            yield scrapy.Request(url, callback=self.parse_page, headers=headers, meta={'capsule': capsule})

    def parse_page(self, response):
        results = response.xpath("//*[contains(@id, 'respProductListPLPCapsule-')]")
 
        txt = results[0].xpath('.//following-sibling::script/text()').extract_first()
        js = re.search(r'window.ui.push\((.*)\)', txt).group(1)
        data = json.loads(js)
        products = data['configuration']['eCommerceData']['products']

        for data in products:
            if data['unitQuantity'] > 1:
                continue

            row = defaultdict()
            row['data'] = data

            row['name'] = data['name']
            row['images'] = ['https://www.nespresso.com' + data['image']['url']]
            row['url'] = 'https://www.nespresso.com' + data['url']
            row['source'] = 'nespresso.com'
            row['brand'] = 'nespresso'
            row['type'] = 'capsule'
            row['system'] = response.meta.get('capsule')
            row['tags'] = [ 'decaf' for lb in data['ranges'] if 'decaffeinated' in lb ]
            row['headline'] = data['headline']
            row['description'] = None
            
            row['price'] = {}
            row['price']['unit'] = int(data['price'])
            #row['price']['min'] = row['price']['unit']
            #row['price']['max'] = row['price']['unit']

            row['flavor'] = {
                'acidity': data['acidity'],
                'bitterness': data['bitterness'],
                'intensity': data['intensity'],
                'body': data['body'],
                'roastiness': data['roastLevel'],
                #'sweetness': 1,
                #'aftertaste': 1,
                'aroma': '',
            }

            row['sizes'] = [ s.split('/')[-1].split('-')[-1] for s in data['cupSizes'] ]
            row['beans'] = ''
            row['weight'] = '20'
            row['coo'] = '스위스'

            row['deleted'] = False
            row['available'] = data['available']
            row['uts'] = Firestore.SERVER_TIMESTAMP
            
            _id = 'NESPRESSO' + str(data['internationalId'].replace('.', ''))

            #Firestore.insert('capsules', _id, row)
            #res = self.collection.find_one_and_replace({ '_id': _id }, data, upsert=True)

            self.append_row([[
                _id, 
                row['brand'], 
                row['system'], 
                row['name'], 
                #','.join(row['tags']), 
                #row['flavor']['bitterness'], row['flavor']['acidity'], row['flavor']['body'], row['flavor']['intensity'], 
                #row['flavor']['roastiness'], 
                #row['flavor']['aroma'],
                #','.join(row['sizes']),
                #row['headline'],
                #row['price']['unit'],
                #row['beans'],
                #row['coo'],
                row['url'],
                row['images'][0]
            ]])

#####

process = CrawlerProcess(settings={
    'USER_AGENT' : 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Mobile Safari/537.36',
    'LOG_LEVEL' : 'WARNING',
    'REFERER': 'https://www.nespresso.com/kr/ko/'
})
crawler = process.create_crawler(Nespresso)
process.crawl(crawler)
process.start()
