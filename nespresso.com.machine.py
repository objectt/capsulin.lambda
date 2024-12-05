# -*- coding: UTF-8 -*-

import json
import re
#import requests 
#import threading
#from contextlib import closing
#from time import time, sleep
#from queue import Queue
from collections import defaultdict
from pymongo import MongoClient, errors

import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import logging
from fs import Firestore

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

        self.collection = self.db.machines
        #self.logger.propagate = False
        #self.logger.setLevel(logging.WARNING)
        self.concurrent_requests = 1
        #self.download_delay = 0.1

    def start_requests(self):
        url = 'https://www.nespresso.com/kr/ko/order/machines/original'

        headers = {
            "Connection": "keep-alive",
        }

        yield scrapy.Request(url, callback=self.parse_page, headers=headers)

    def parse_page(self, response):
        results = response.xpath("//*[contains(@id, 'respProductListPLPMachine-')]")
 
        txt = results[0].xpath('.//following-sibling::script/text()').extract_first()
        js = re.search(r'window.ui.push\((.*)\)', txt).group(1)
        data = json.loads(js)
        products = data['configuration']['eCommerceData']['products']

        for data in products:
            row = defaultdict()
            row['data'] = data
            row['id'] = str(data['internationalId'])
            row['name'] = data['name']
            row['price'] = int(data['price'])
            row['image'] = 'https://www.nespresso.com' + data['image']['url']
            row['url'] = 'https://www.nespreso.com' + data['url']
            row['source'] = 'nespresso.com'
            row['type'] = 'machine'
            row['brand'] = 'nespresso'
            row['system'] =  data['technologies'][0].split('/')[-1]

            row['available'] = data['available']
            row['deleted'] = False
            
            _id = 'NSP-' + row['id']
            Firestore.insert('machines', _id, row)
            #res = self.collection.find_one_and_replace({ '_id': _id }, row, upsert=True)

#####

process = CrawlerProcess(settings={
    'USER_AGENT' : 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Mobile Safari/537.36',
    'LOG_LEVEL' : 'WARNING',
    'REFERER': 'https://www.nespresso.com/kr/ko/'
})
crawler = process.create_crawler(Nespresso)
process.crawl(crawler)
process.start()
