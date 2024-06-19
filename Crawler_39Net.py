import concurrent.futures
import re

import requests
import tqdm

from settings import headers,cookies
from import_rbq import RBQ_Client
from BloomFilter import BloomFilter
from lxml import etree
from concurrent.futures.process import ProcessPoolExecutor
class Crawler_39Net:


    def __init__(self):

        self.wander_rbq=RBQ_Client("39net_wander")
        self.tasks_rbq=RBQ_Client("39net_tasks")

        self.rege_expr="https?://.*39\.net/\w+/\d+/\w+\.html"
        self.wander_bloom=BloomFilter("wander",1000_000_000, 5)
        self.tasks_bloom=BloomFilter("tasks",1000_000_000, 5)

        self.extract_link_xpath="//a/@href"

    def run(self):

        while True:
            method,msg=self.wander_rbq.get_message()
            if msg is None:
                print("completed!!!!!")
                break
            msg=msg.decode().replace("\"","")
            self.get(method,msg)
    def get(self,method,url):

        while True:
            try:
                response=requests.get(url,headers=headers,cookies=cookies,timeout=10)
                tree = etree.HTML(response.text)
                link_ls=tree.xpath(self.extract_link_xpath)
                link_ls=list(set(link_ls))
                for link in tqdm.tqdm(link_ls,desc=url):
                    if re.match(self.rege_expr,link) is not None:
                        if  link not in self.tasks_bloom:
                            self.tasks_bloom.add(link)
                            self.tasks_rbq.publish_message(link)
                    elif "39.net" in link:
                        if link not in self.wander_bloom:
                            self.wander_bloom.add(link)
                            self.wander_rbq.publish_message(link)

                self.wander_rbq.complete(method)
                break
            except ConnectionError:
                print("连接超时")
                break

    @staticmethod
    def create_and_run():
        ins = Crawler_39Net()
        ins.run()

if __name__ == '__main__':
    pool=ProcessPoolExecutor(max_workers=80)
    task_ls=[]
    for x in range(80):
        task_ls.append(pool.submit(Crawler_39Net.create_and_run))

    concurrent.futures.wait(task_ls)