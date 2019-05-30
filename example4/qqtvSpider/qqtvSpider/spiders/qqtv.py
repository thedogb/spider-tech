# -*- coding: utf-8 -*-
import scrapy

headers = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}

class QqtvSpider(scrapy.Spider):
    name = 'qqtv'
    allowed_domains = ['v.qq.com']
    start_urls = ['http://v.qq.com/']

    def start_requests(self):
        raw_url = "https://v.qq.com/x/bu/pagesheet/list?append=1&channel=movie&itype=100062&listpage=2&offset=%d&pagesize=%d"

        pagesize = 30
        for offset in range(0, 500, pagesize):
            url = raw_url%(offset, pagesize)
            request = scrapy.Request(
                url=url,
                headers=headers,
                dont_filter=False,
                callback=self.parse_url,
                errback=self.parse_err
            )
            yield request

    def parse_url(self, response):
        urls = [x.extract() for x in response.xpath("//a/@href")]
        for url in urls:
            request = scrapy.Request(
                url=url,
                headers=headers,
                dont_filter=False,
                callback=self.parse,
                errback=self.parse_err
            )
            yield request

    def parse_err(self, response):
        url = response.request.url
        item = {
            "msg": "error",
            "info": url
        }
        yield item

    def parse(self, response):
        title = response.xpath('//h1[@class="video_title _video_title"]/text()')
        content = response.xpath('//p[@class="summary"]/text()')

        title = list(filter(lambda x: x!="", [x.extract().strip() for x in title]))
        content = list(filter(lambda x: x!="",[x.extract().strip() for x in content]))

        content = " ".join(content)

        result = {
            "title": ",".join(title),
            "content": content
        }

        item = {
            "msg": "success",
            "info": result
        }
        yield item

