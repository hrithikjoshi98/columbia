import hashlib
from typing import Iterable
from urllib.parse import urlparse
import parsel
import scrapy
import json

from scrapy import Request
from scrapy.cmdline import execute
from columbia.items import ColumbiaItem
from columbia.db_config import config
import pymysql
from datetime import datetime
import os
import gzip
from parsel import Selector
import re

def remove_extra_space(row_data):
    # Remove any extra spaces or newlines created by this replacement
    value = re.sub(r'\s+', ' ', row_data).strip()
    # Update the cleaned value back in row_data
    return value


def generate_hashid(url: str) -> str:
    # Parse the URL and use the netloc and path as a unique identifier
    parsed_url = urlparse(url)
    unique_string = parsed_url.netloc + parsed_url.path
    # Create a hash of the unique string using SHA-256 and take the first 8 characters
    hash_object = hashlib.sha256(unique_string.encode())
    hashid = hash_object.hexdigest()[:8]  # Take the first 8 characters
    return hashid

class ColumbSpider(scrapy.Spider):
    name = "columb"
    start_urls = ["https://stores.columbia.com/"]

    def __init__(self, start_id, end_id, **kwargs):
        super().__init__(**kwargs)
        self.start_id = start_id
        self.end_id = end_id

        self.conn = pymysql.connect(
            host=config.host,
            user=config.user,
            password=config.password,
            db=config.database,
            autocommit=True
        )
        self.cur = self.conn.cursor()

        self.domain = self.start_urls[0].split('://')[1].split('/')[0]
        self.date = datetime.now().strftime('%d_%m_%Y')

        self.folder_name = self.domain.replace('.', '_').strip()
        config.file_name = self.folder_name

        self.html_path = 'C:\page_source\\' + self.date + '\\' + self.folder_name + '\\'
        if not os.path.exists(self.html_path):
            os.makedirs(self.html_path)
        # print(self.domain, self.folder_name, self.sql_table_name)
        self.sql_table_name = self.folder_name + f'_{self.date}' + '_USA'

        config.db_table_name = self.sql_table_name

        print(self.sql_table_name)
        field_list = []
        value_list = []
        item = ('store_no', 'name', 'latitude', 'longitude', 'street', 'city',
                  'state', 'zip_code', 'county', 'phone', 'open_hours', 'url',
                  'provider', 'category', 'updated_date', 'country', 'status',
                  'direction_url', 'pagesave_path')
        for field in item:
            field_list.append(str(field))
            value_list.append('%s')
        config.fields = ','.join(field_list)
        config.values = ", ".join(value_list)

        self.cur.execute(f"""CREATE TABLE IF NOT EXISTS {self.sql_table_name}(id int AUTO_INCREMENT PRIMARY KEY,
                                    store_no varchar(100) DEFAULT 'N/A',
                                    name varchar(100) DEFAULT 'N/A',
                                    latitude varchar(100) DEFAULT 'N/A',
                                    longitude varchar(100) DEFAULT 'N/A',
                                    street varchar(500) DEFAULT 'N/A',
                                    city varchar(100) DEFAULT 'N/A',
                                    state varchar(100) DEFAULT 'N/A',
                                    zip_code varchar(100) DEFAULT 'N/A',
                                    county varchar(100) DEFAULT 'N/A',
                                    phone varchar(100) DEFAULT 'N/A',
                                    open_hours varchar(500) DEFAULT 'N/A',
                                    url varchar(500) DEFAULT 'N/A',
                                    provider varchar(100) DEFAULT 'N/A',
                                    category varchar(100) DEFAULT 'N/A',
                                    updated_date varchar(100) DEFAULT 'N/A',
                                    country varchar(100) DEFAULT 'N/A',
                                    status varchar(100) DEFAULT 'N/A',
                                    direction_url varchar(500) DEFAULT 'N/A',
                                    pagesave_path varchar(500) DEFAULT 'N/A'
                                    )""")

    def parse(self, response, **kwargs):
        selector = Selector(response.text)

        lis = selector.xpath('//li/a[contains(@class,"Link font-avenir text-base font-[550] text-gray-900")]/@href').getall()

        for state_url in lis:
            yield scrapy.Request(
                self.start_urls[0] + state_url,
                                 callback=self.get_city
            )

    def get_city(self, response):
        selector = Selector(response.text)

        for city_url in selector.xpath('//li/a[contains(@class,"Link group font-avenir text-base font-[550] '
                                       'text-gray-900")]/@href').getall():
            yield scrapy.Request(
                self.start_urls[0] + city_url,
                callback=self.get_store
            )

    def get_store(self, response):
        selector = Selector(response.text)

        for store_url in selector.xpath('//div[@id="reactele"]//ul//li[@class="flex w-[296px] flex-col gap-4 border '
                                    'border-solid border-gray-400 bg-white p-4 mx-auto"]//h3//a/@href').getall():
            yield scrapy.Request(
                self.start_urls[0] + store_url.replace('.',''),
                callback=self.store_detail_page
            )

    def store_detail_page(self, response):
        selector = Selector(response.text)
        item = ColumbiaItem()

        json_data = selector.xpath('//script[@type="application/ld+json"]//text()').get()
        json_data = json.loads(json_data)

        try:
            store_no = json_data['@graph'][1]['identifier']
        except Exception as e:
            store_no = 'N/A'

        try:
            name = json_data['@graph'][1]['name']
        except Exception as e:
            name = 'N/A'

        try:
            latitude = json_data['@graph'][2]['latitude']
        except Exception as e:
            latitude = 'N/A'

        try:
            longitude = json_data['@graph'][2]['longitude']
        except Exception as e:
            longitude = 'N/A'

        try:
            street = json_data['@graph'][2]['address']['streetAddress']
        except Exception as e:
            street = 'N/A'

        try:
            city = json_data['@graph'][2]['address']['addressLocality']
        except Exception as e:
            city = 'N/A'

        try:
            state = json_data['@graph'][2]['address']['addressRegion']
        except Exception as e:
            state = 'N/A'

        try:
            zip_code = json_data['@graph'][2]['address']['postalCode']
        except Exception as e:
            zip_code = 'N/A'

        county = 'N/A'

        try:
            phone = json_data['@graph'][2]['telephone']
        except Exception as e:
            phone = 'N/A'

        try:
            o_h = json_data['@graph'][2]['openingHours']
            o_h = ' | '.join(o_h)
            days = {
                'Mo': 'Monday', 'Tu': 'Tuesday', 'We': 'Wednesday',
                'Th': 'Thursday', 'Fr': 'Friday', 'Sa': 'Saturday', 'Su': 'Sunday'
            }
            for i in days:
                if i in o_h:
                    o_h = o_h.replace(i, days[i]+':')
            open_hours = o_h
        except Exception as e:
            open_hours = 'N/A'

        try:
            direction_url = selector.xpath('//a[contains(@class, "inline-block") and contains(text(), "Get Directions")]/@href').get()
        except Exception as e:
            direction_url = 'N/A'

        try:
            status = selector.xpath('//*[@id="reactele"]//span[@class="flex h-[30px] items-center justify-center bg-primary-black px-[5px] py-[3px] text-xl font-[750]"]//text()').get('N/A')

            if 'Open' in status:
                status = 'Open'

        except Exception as e:
            status = 'N/A'


        url = response.url

        provider = 'Columbia Sportswear'
        category = 'Apparel And Accessory Stores'

        updated_date = datetime.now().strftime("%d-%m-%Y")
        country = 'USA'

        page_id = generate_hashid(response.url)
        pagesave_path = self.html_path + fr'{page_id}' + '.html.gz'

        gzip.open(pagesave_path, "wb").write(response.body)

        item['store_no'] = store_no
        item['name'] = name
        item['latitude'] = latitude
        item['longitude'] = longitude
        item['street'] = street
        item['city'] = city
        item['state'] = state
        item['zip_code'] = zip_code
        item['county'] = county
        item['phone'] = phone
        item['open_hours'] = open_hours
        item['url'] = url
        item['provider'] = provider
        item['category'] = category
        item['updated_date'] = updated_date
        item['country'] = country
        item['status'] = status
        item['direction_url'] = direction_url
        item['pagesave_path'] = pagesave_path
        yield item



if __name__ == '__main__':
    # execute("scrapy crawl kia".split())
    execute(f"scrapy crawl columb -a start_id=0 -a end_id=100 -s CONCURRENT_REQUESTS=6".split())