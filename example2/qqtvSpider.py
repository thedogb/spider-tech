#coding=utf-8

"""
这是一个用来爬取腾讯视频的爬虫例程，使用requests实现，单线程。
爬取
    https://v.qq.com/channel/movie?listpage=1&channel=movie&itype=100062
上面这个页面中的5000个电影的信息
"""

import requests
from lxml import etree
import json
import time

def download(url, parser_fn=None, delay=0.1):
    """
    下载页面，调用解析函数对页面进行解析。若解析函数为None，则直接返回页面
    :param url: 待下载的url
    :param parser_fn: 解析函数, 其返回值为(urls, items)
    :return: 下载成功，返回解析后的数据; 反之，返回None
    """

    headers = {'User-Agent': 'Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'}

    try:
        r = requests.get(url, headers)
    except Exception as e:
        return None

    time.sleep(delay)

    text = r.content

    if parser_fn == None:
        return text
    else:
        return parser_fn(text)

def parse_url(text):
    html = etree.HTML(text)
    urls = html.xpath("//a/@href")
    return set(urls)


def parse(text):
    """
    从下载好的页面内容中解析出下一步要爬取的url以及该页面本身的文字内容
    :param text: 下载好的页面
    :return: content
    """

    html = etree.HTML(text)

    title = html.xpath('//h1[@class="video_title _video_title"]/text()')
    content = html.xpath('//p[@class="summary"]/text()')

    title = list(filter(lambda x: x!="", [x.strip() for x in title]))
    content = list(filter(lambda x: x!="",[x.strip() for x in content]))

    content = " ".join(content)

    item = {}
    item['title'] = ",".join(title)
    item['content'] = content

    return item

def write(f, item):
    """
    将爬取的内容写入到文件当中
    :param f: 待写入的文件指针
    :param item: 待写入的数据
    :return: None
    """

    # 由于我们要写入到一个文件中，所以这里要进行一些处理
    for k in item:
        v = item[k].replace("\n", " ")
        item[k] = v

    line = json.dumps(item, ensure_ascii=False)
    f.write(line+"\n")

def main():
    rest_urls = set(["https://v.qq.com/channel/movie"])
    host = "https://v.qq.com"
    error_urls = set([])
    finished_urls = set([])

    out_f = open("./out/result.json", 'w')
    error_f = open("./out/error.csv", 'w')
    finished_f = open("./out/finished.csv", 'w')

    raw_url = "https://v.qq.com/x/bu/pagesheet/list?append=1&channel=movie&itype=100062&listpage=2&offset=%d&pagesize=%d"
    pagesize = 30

    # 1. 获取五千部电影的链接
    print("download urls...")
    for offset in range(0, 500, pagesize):
        print("%d urls have been downloaded!" % offset)
        url = raw_url%(offset, pagesize)
        urls = download(url, parse_url)
        rest_urls |= urls

    # 2. 开始下载每部电影信息
    num = 0
    for url in rest_urls:
        print("%d urls have been downloaded, downloading %s..." % (num, url))
        item = download(url, parser_fn=parse)

        if item == None:
            error_urls.add(url)
        else:
            finished_urls.add(url)
            num += 1

            content = item

            # 将数据写入文件
            write(out_f, content)

    # 爬取完成，记录结果
    for url in error_urls:
        error_f.write(url+'\n')

    for url in finished_urls:
        finished_f.write(url+'\n')

if __name__ == '__main__':
    import time
    start_time = time.time()
    main()
    print("finished: ", time.time() - start_time)

