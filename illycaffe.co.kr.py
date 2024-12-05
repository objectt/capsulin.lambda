# -*- coding: UTF-8 -*-

import json
import re
from collections import defaultdict
from pymongo import MongoClient, errors
import datetime

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import logging
from fs import Firestore

import boto3

class Illy(scrapy.Spider):
    name = 'Illy'

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
        #self.logger.propagate = False
        #self.logger.setLevel(logging.WARNING)
        self.concurrent_requests = 1
        #self.download_delay = 0.1

        self.s3 = boto3.client('s3')

    def upload_image():
        s3path = 'capsulelin/'
        self.s3.put_object(ACL='public-read', Body=fileout, Key=s3path + filename, Bucket='img.teetime.cc', ContentType='image/png')

    def start_requests(self):
        urls = [ 
            'https://shop.illycaffe.co.kr/goods/goods_list.php?cateCd=001008',
            #'https://shop.illycaffe.co.kr/goods/goods_list.php?page={0}&cateCd=001006&sort=&pageNum=48',
            #'https://shop.illycaffe.co.kr/goods/goods_list.php?page={0}&cateCd=001003&sort=&pageNum=48'
        ]

        headers = {
            "Connection": "keep-alive",
        }

        for url in urls:
            for page in range(1, 2):
                yield scrapy.Request(url.format(page), callback=self.parse_page, headers=headers)

    def parse_page(self, response):
        ul = response.css('.item_basket_type')
        lis = ul.css('li')

        for li in lis:
            row = defaultdict(str)
            row['name'] = li.css('strong::text').get().strip()

            if row['name'] == 'SOLD OUT' or '캡슐' not in row['name'] or '총 ' in row['name']:
                continue

            anchor = li.css('a')
            href = anchor.attrib['href'].replace('..', '')

            row['data'] = {}
            row['id'] = href.split('=')[-1]
            #row['image'] = 'https://shop.illycaffe.co.kr' + li.css('img').attrib['src']
            row['image'] = li.css('img').attrib['src']
            row['url'] = 'https://shop.illycaffe.co.kr' + href
            row['source'] = 'illycaffe.co.kr'
            row['brand'] = 'illy'
            row['type'] = 'capsule'
            row['system'] = 'nespresso'
            
            row['price'] = {}
            row['price']['unit'] = int(li.css('.item_price ::text').extract()[1].strip().replace(',','').replace('원',''))
            row['price']['pack'] = int(li.css('.item_price ::text').extract()[1].strip().replace(',','').replace('원',''))

            if '21P' in row['name']:
                row['price']['unit'] = int(row['price']['pack'] / 21)
            elif '18P' in row['name']:
                row['price']['unit'] = int(row['price']['pack'] / 18)
            elif '호환' in row['name']:
                row['price']['unit'] = int(row['price']['pack'] / 10)
            
            row['price']['min'] = row['price']['unit']
            row['price']['max'] = row['price']['unit']

            #row['name'] = row['name'].replace('21P', '').replace('18P', '')
            #row['name'] = re.sub('[()\[\]]', '', row['name'].replace('캡슐커피', ''))
            row['name'] = re.findall('\[(.*?)\]', row['name'])[0].split(':')[0]
            row['tags'] = [ 'decaf' ] if '디카프' in row['name'] else []
            row['uts'] = Firestore.SERVER_TIMESTAMP
            row['deleted'] = False
            row['available'] = True

            _id = 'ILLY' + str(row['id'])
            Firestore.insert('capsules', _id, row)
#####

process = CrawlerProcess(settings={
    'UER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Safari/537.36',
    'LOG_LEVEL' : 'WARNING',
    'REFERER': 'https://shop.illycaffe.co.kr'
})
crawler = process.create_crawler(Illy)
process.crawl(crawler)
process.start()
