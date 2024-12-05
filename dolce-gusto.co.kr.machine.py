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

class Nescafe(scrapy.Spider):
    name = 'Nescafe'

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
        url = 'https://www.dolce-gusto.co.kr/machines'

        headers = {
            "Connection": "keep-alive",
        }

        yield scrapy.Request(url, callback=self.parse_page, headers=headers)

    def parse_page(self, response):
        results = response.css('ul.category-products')
        lis = results.css('li')

        for li in lis:
            anchor = li.css('a')
            anchor2 = li.css('h2.product-name a')

            row = defaultdict()
            row['name'] = anchor2.attrib['title']
            row['price'] = li.css('.price ::text').extract_first().replace(',', '').strip()
            row['price'] = int(re.findall(r'\d+', row['price'])[0]) if row['price'] else 0
            row['image'] = li.css('img').attrib['data-echo']
            row['url'] = anchor.attrib['href']
            row['id'] = row['url'].split('/')[-1].upper()
            row['source'] = 'dolce-gusto.co.kr'
            row['type'] = 'machine'
            row['system'] = 'dolce'
            row['brand'] = 'nescafe'
            row['deleted'] = False
            row['available'] = True
            
            _id = 'NESCAFE-' + str(row['id'])
            Firestore.insert('machines', _id, row)
            #res = self.collection.find_one_and_replace({ '_id': _id }, row, upsert=True)

#####

process = CrawlerProcess(settings={
    'USER_AGENT' : 'Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.88 Mobile Safari/537.36',
    'LOG_LEVEL' : 'WARNING',
    'REFERER': 'https://www.nespresso.com/kr/ko/'
})
crawler = process.create_crawler(Nescafe)
process.crawl(crawler)
process.start()
