# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import time

class QqtvspiderPipeline(object):
    def open_spider(self, spider):
        self.out_f = open("./out/result.json", 'w')
        self.error_f = open("./out/error.csv", 'w')
        self.start_time = time.time()

    def process_item(self, item, spider):
        if item['msg'] == 'error':
            url = item['info']
            self.error_f.write(url+"\n")
        elif item['msg'] == 'success':
            result = item['info']
            line = json.dumps(result, ensure_ascii=False)+'\n'
            self.out_f.write(line)
        return item

    def close_spider(self, spider):
        self.out_f.close()
        self.error_f.close()
        print("spider finished:", time.time()-self.start_time)
