# coding=utf-8

import requests
from bs4 import BeautifulSoup

ret = requests.get("https://www.okex.com/support/hc/zh-cn/categories/115000275131")
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["最新公告", "最新活动", "冲提公告", "法币公告", "币币/杠杆公告", "合约公告", "OKEX云", "矿池公告"]
article_list_arr = soup.find_all(name='ul', attrs={"class": "article-list"})
for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='li', attrs={"class": "article-list-item"})
    for item in arr:
        print(item.text.strip())
