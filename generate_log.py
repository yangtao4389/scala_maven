# coding=utf-8

import random
import time
import os
# from urllib import parse

url_paths = [
    "class/154.html",
    "class/128.html",
    "class/147.html",
    "class/116.html",
    "class/138.html",
    "class/140.html",
    "learn/828",
    "learn/521",
    "course/list"
]

ip_slices = [127, 156, 222, 105, 24, 192, 153, 127, 31, 168, 32, 10, 82, 77, 118, 228]

http_referers = [
    "http://www.baidu.com/s?wd={query}",
    "https://www.sogou.com/web?query={query}",
    "http://cn.bing.com/search?q={query}",
    "https://search.yahoo.com/search?p={query}",
]

search_keyword = [
    "Spark 项目实战",
    "Hadoop 项目实战",
    "Storm 项目实战",
    "Spark Streaming 项目实战",
    "Hive 项目实战"
]

status_codes = ["200", "404", "500", "503", "403"]


def sample_url():
    return random.sample(url_paths, 1)[0]


def sample_ip():
    slice = random.sample(ip_slices, 4)
    return ".".join([str(item) for item in slice])


def sample_referer():
    if random.uniform(0, 1) > 0.2:
        return "-"

    refer_str = random.sample(http_referers, 1)
    query_str = random.sample(search_keyword, 1)
    # quote_zhongwen = parse.quote(query_str[0])
    return refer_str[0].format(query=query_str[0])



def sample_status_code():
    return random.sample(status_codes, 1)[0]


def generate_log(count=10):
    time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

    parent_dir = "/home/code/dev/scala_log/nginx/"
    if not os.path.exists(parent_dir):
        os.makedirs(parent_dir)
    with open(parent_dir +"access.log", "w+") as f:
        while count >= 1:
            query_log = "{ip}\t{local_time}\t\"GET /{url} HTTP/1.1\"\t{status_code}\t{referer}".format(url=sample_url(),
                                                                                                       ip=sample_ip(),
                                                                                                       referer=sample_referer(),
                                                                                                       status_code=sample_status_code(),
                                                                                                       local_time=time_str)
            print(query_log)
            f.write(query_log + "\n")
            count = count - 1


if __name__ == '__main__':
    generate_log(100)