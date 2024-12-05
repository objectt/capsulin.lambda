import os
import re
from datetime import datetime, timedelta
from collections import defaultdict

import requests
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.log import logging

import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
from fs import Firestore

class RobotPP(scrapy.Spider):
    name = 'ppomppu.co.kr'

    def __init__(self, *args, **kwargs):
        scrapy.Spider.__init__(self)

        self.logger.propagate = False
        self.logger.setLevel(logging.WARNING)
        
        self.curPath = os.path.dirname(os.path.abspath(__file__))
        keyFile = self.curPath + "/serviceAccountKey.json"
        
        if (not len(firebase_admin._apps)):
            self.cred = credentials.Certificate(keyFile)
            firebase_admin.initialize_app(self.cred)
        
        self.fs = firestore.client()
        self.ts = datetime.now().strftime('%y%m%d%H%M')

    def start_requests(self):
        for pageIndex in range(1,20):
            url = 'https://www.ppomppu.co.kr/zboard/zboard.php?id=ppomppu&category=6&divpage=66&page={0}'.format(pageIndex)
            yield scrapy.Request(url, callback=self.parse_list)

    def parse_list(self, response):
        table = response.css('#revolution_main_table')
        trs = table.css('tr')
        i = 0

        for tr in trs[3:]:
            classes = tr.xpath('@class').extract()
            if not classes or 'list_notice' in classes[0]:
                continue

            tds = tr.css('td.list_vspace')
            data = defaultdict(str)

            if not tds:
                continue

            id = tds[0].xpath('./text()')
            if not id:
                continue

            data['source'] = 'ppomppu.co.kr'
            data['id'] = int(id.get().strip())
            #data['category'] = None
            #data['author'] = tds[1].css('span.list_name::text').get()

            anchor = tds[2].css('a')[-1]
            data['title_raw'] = anchor.css('.list_title::text').get()

            if not data['title_raw']:
                continue

            pattern = re.compile(r'\[.*?\]|\(.*?\)')
            parts = re.findall(pattern, data['title_raw'])
            if not parts:
                continue

            data['host'] = parts[0][1:-1]
            data['title'] = re.sub(pattern, '', data['title_raw']).strip()
            data['url'] = 'https://www.ppomppu.co.kr/zboard/' + anchor.attrib['href']

            data['ts'] = tds[3].attrib['title']
            data['ts'] = datetime.strptime(data['ts'], "%y.%m.%d  %H:%M:%S") - timedelta(hours=9, minutes=0)
            #data['uts'] = datetime.utcnow() #.strftime('%Y-%m-%d %H:%M:%S')
            data['uts'] = Firestore.SERVER_TIMESTAMP

            votes = tds[4].xpath('./text()').get()

            if any(k in data['title'] for k in ('네스프레소', '캡슐', '커피')):
                yield scrapy.Request(data['url'], callback=self.parse_page, cb_kwargs=dict(data=data))

    def parse_page(self, resp, data):
        response = resp.css('.sub-top-text-box')

        data['hot'] = True if response.css('.view_title2 img') else False
        data['link'] = response.css('.wordfix a::text').extract_first()
        data['deleted'] = False
        data['brand'] = 'etc'

        if '네스프레소' in data['title']:
            data['brand'] = 'nespresso'
        if '일리' in data['title']:
            data['brand'] = 'illy'
        if '스타벅스' in data['title']:
            data['brand'] = 'starbucks'
        if '네스카페' in data['title']:
            data['brand'] = 'nescafe'

        #data['message'] = body.xpath('.//text()').extract()
        #data['message'] = ' '.join([ p.strip() for p in data['message'] if p.strip()])

        if (data['link']):
            yield scrapy.Request(data['link'], callback=self.parse_meta, cb_kwargs=dict(data=data), headers={ "User-Agent": "yeti" })
            
    def parse_meta(self, response, data):
        #for i in ['ogtitle', 'ogdescription', 'ogimage']:
        for i in ['image']:
            xpath = "//meta[@property='og:{0}']/@content".format(i)
            content = response.xpath(xpath).extract_first()
            if content:
                data[i] = content if content.startswith('http') else data['link'].split('/')[0] + content

        eid = 'pp_' + str(data['id'])
        self.fs.collection(u'deals').document(eid).set(data, merge=True)

#################################

process = CrawlerProcess(settings={
    'REFERER': 'https://www.ppomppu.co.kr',
    'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36',
    'LOG_LEVEL': 'WARNING'
})
crawler = process.create_crawler(RobotPP)
crawler.stats.set_value('total', 0)
process.crawl(crawler)
process.start()

stats = crawler.stats.get_stats()
#slack("{0} ppomppu.co.kr crawled".format(stats['total'] if 'total' in stats else 0))
