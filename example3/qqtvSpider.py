#coding=utf-8

"""
这是一个用来爬取腾讯视频的爬虫例程，使用requests实现，单线程。
爬取
    https://v.qq.com/channel/movie?listpage=1&channel=movie&itype=100062
上面这个页面中的5000个电影的信息
"""

import multiprocessing
from multiprocessing import Manager, Pool, Queue
import requests
from lxml import etree
import json
import time


# 定义一个全局任务队列
queue = Manager().list()
id_queue = Queue()


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

def process_one(pid, task):
    task_id, url = task
    if task_id == 0:
        urls = download(url, parse_url)
        for new_url in urls:
            # 将新的url作为任务加入任务池
            queue.append((1, new_url))

    elif task_id == 1:
        item = download(url, parser_fn=parse)
        if item == None:
            err_path = "./out/error_%d.csv"%pid
            err_f = open(err_path, 'a')
            err_f.write(url+'\n')
            err_f.close()
        else:
            content = item
            out_f = open("./out/result_%d.json"%pid, 'a')
            write(out_f, content)
            out_f.close()

    # 完成后，将pid释放
    id_queue.put(pid, block=False)




def main():
    # 1. 将展示页链接放入任务池
    raw_url = "https://v.qq.com/x/bu/pagesheet/list?append=1&channel=movie&itype=100062&listpage=2&offset=%d&pagesize=%d"
    pagesize = 30
    for offset in range(0, 500, pagesize):
        url = raw_url%(offset, pagesize)
        queue.append((0, url))

    cpu_num = multiprocessing.cpu_count() * 2
    # 2. 向id_queue存入编号
    for i in range(cpu_num):
        id_queue.put(i)

    # 3. 开始多进程下载
    print("total cpu num: %d"%cpu_num)
    pool = Pool(processes=cpu_num)

    while True:
        task = queue.pop(0)
        pid = id_queue.get(block=True, timeout=600)
        print("分配任务给进程：%d, %s"%(pid, task[1]))
        pool.apply_async(process_one, (pid, task))

        if len(queue) == 0:
            time.sleep(5)
            if len(queue) == 0:
                break

    print("waiting download finsh...")
    pool.close()
    pool.join()




if __name__ == '__main__':
    import time
    start_time = time.time()
    main()
    print("finished: ", time.time() - start_time)

