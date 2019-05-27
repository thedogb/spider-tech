#coding=utf-8

"""
这是一个用来爬取萌娘百科的爬虫例程，使用requests进行广度优先搜索实现，单线程。
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

    text = r.text

    if parser_fn == None:
        return text
    else:
        return parser_fn(text)

def parse(text):
    """
    从下载好的页面内容中解析出下一步要爬取的url以及该页面本身的文字内容
    :param text: 下载好的页面
    :return: (urls, content)
    """

    html = etree.HTML(text)

    urls = html.xpath("//a/@href")

    urls = filter(lambda x: "/wiki/" in x and "/wiki/File:" not in x and "/wiki/Image:" not in x and x[0] != '#', urls)

    title = html.xpath('//h1[@class="firstHeading"]/text()')
    content = html.xpath("//p//text()")
    content = " ".join(content)

    item = {}
    item['title'] = ",".join(title)
    item['content'] = content

    return (urls, item)

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
    rest_urls = set(["https://wiki.mbalib.com/wiki/%E9%A6%96%E9%A1%B5"])
    host = "https://wiki.mbalib.com"
    error_urls = set([])
    finished_urls = set([])

    out_f = open("./out/result.json", 'w')
    error_f = open("./out/error.csv", 'w')
    finished_f = open("./out/finished.csv", 'w')

    max_num = 300
    num = 0

    while(len(rest_urls) > 0):
        url = rest_urls.pop()
        print("%d urls have been downloaded, downloading %s..." % (num, url))
        item = download(url, parser_fn=parse)
        if item == None:
            error_urls.add(url)
        else:
            finished_urls.add(url)
            num += 1

            urls, content = item

            # 将数据写入文件
            write(out_f, content)

            # 将新的url加入待爬取的集合中
            for url in urls:
                if 'https://' not in url:
                    url = host+url
                if url not in finished_urls|error_urls|rest_urls:
                    rest_urls.add(url)

            if num > max_num:
                break

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

