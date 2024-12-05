# -*- coding: UTF-8 -*-

import json
import re
from collections import defaultdict
from pymongo import MongoClient, errors

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import logging
from fs import Firestore

import boto3
from httplib2 import Http
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials

class Dolce(scrapy.Spider):
    name = 'Dolce'

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

        self.collection = self.db.nespresso
        self.concurrent_requests = 1

        #self.s3 = boto3.client('s3')

        SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
        creds = ServiceAccountCredentials.from_json_keyfile_name('./serviceAccountKey.json', SCOPES)
        http_auth = creds.authorize(Http())
        service = build('sheets', 'v4', credentials=creds)
        self.sheet = service.spreadsheets()
    
    def append_row(self, values):
        SPREADSHEET_ID = '1JZSbnesd5OGkk44GVRW4igKgwEso6oulG8S1JxFON3M'
        RANGE_NAME = 'crawler!A2'

        body = { 'values': values }
        self.sheet.values().append(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME, valueInputOption='USER_ENTERED', body=body).execute()

    def upload_image():
        s3path = 'capsulelin/'
        self.s3.put_object(ACL='public-read', Body=fileout, Key=s3path + filename, Bucket='img.teetime.cc', ContentType='image/png')

    def start_requests(self):
        urls = [ 'https://dolce-gusto.co.kr/flavours' ]

        headers = {
            "Connection": "keep-alive",
            "Referer": "https://dolce-gusto.co.kr"
        }

        for url in urls:
            yield scrapy.Request(url, callback=self.parse_page, headers=headers)

    def parse_page(self, response):
        #ul = response.css('.category-products')
        ul = response.css('.products-listing__list.products')
        lis = ul.css('li.item')

        for li in lis:
            row = defaultdict(str)

            card = ul.css('.product-card')
            info = card.css('.product-card__info')

            row['name'] = li.css('a.product-card__name--link::text').extract_first().strip()

            if any(k in row['name'] for k in ['Value', 'VALUE', '컬렉션팩', '상품권']):
                continue

            price = li.css('.price-final_price').css('.price-wrapper')
            if not price:
                continue

            row['price'] = { 
                'total': int(price.attrib['data-price-amount']) 
            }
            #row['price'] = int(re.findall(r'\d+', price.replace(',', ''))[0]) if price else 0

            if row['price'] == 0:
                continue

            #size = info.css('.product-card__capsules span')[1]
            #size = size.css('b::text').extract_first()

            anchor = li.css('a')
            href = anchor.attrib['href']

            row['data'] = None
            row['id'] = card.attrib['id'] #href.split('/')[-1].upper()
            row['image'] = li.css('img').attrib['data-src']
            row['url'] = href
            row['source'] = 'dolce-gusto.co.kr'
            row['brand'] = 'nescafe'
            row['type'] = 'capsule'
            row['system'] = 'dolce'
            row['tags'] = ['decaf' ] if '디카페인' in row['name'] else []
            
            if '스타벅스' in row['name']:
                row['tags'].append('starbucks')

            row['deleted'] = False
            row['available'] = row['available']
            row['uts'] = Firestore.SERVER_TIMESTAMP
            
            headers = {}
            yield scrapy.Request(row['url'], callback=self.parse_product_page, headers=headers, meta={'data': row})

    def parse_product_page(self, response):
        data = response.meta.get('data')
        product = response.css('.product__information')

        data['headline'] = product.css('.product__subtitle h2::text').extract_first()
        data['description'] = product.css('.product.attribute.overview p::text').extract()
        
        data['headline'] = data['headline'].strip() if data['headline'] else None
        data['description'] = " ".join(data['description']).strip() if data['description'] else None

        intensity = product.css('.product__intensity--number::text').extract_first()
        quantity = product.css('.quantity-number::text').extract_first().strip()

        data['quantity'] = int(re.findall(r'\d+', quantity.strip())[0]) if quantity else None
        data['price']['unit'] = data['price']['total'] / data['quantity']

        data['flavor'] = {
            'acidity': None,
            'bitterness': None,
            'aroma': None,
            'sweetness': None,
            'aftertaste': None,
            'intensity': intensity.strip() if intensity else None
        }

        data['roast'] = {
            'level': None
        }

        self.append_row([[
            data['id'],
            data['brand'], 
            data['system'], 
            data['name'],
            data['flavor']['intensity'],
            data['price']['unit'], 
            data['headline'], 
            #",".join(data['tags']), 
            data['url'],
            data['image']
        ]])
#####

process = CrawlerProcess(settings={
    'UER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'LOG_LEVEL' : 'WARNING',
    'REFERER': 'https://dolce-gusto.co.kr'
})
crawler = process.create_crawler(Dolce)
process.crawl(crawler)
process.start()
