# coding=utf-8

import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup

# ret = requests.get("https://support.hbfile.net/hc/zh-cn/categories/360000031902-%E9%87%8D%E8%A6%81%E5%85%AC%E5%91%8A")
# print(ret.text)

headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
}
# 重要公告
url = "https://support.hbfile.net/hc/zh-cn/categories/360000031902-%E9%87%8D%E8%A6%81%E5%85%AC%E5%91%8A"
ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["最新热点", "HT专栏", "冲提公告", "新币上线", "项目介绍", "API公告", "其他"]
article_list_arr = soup.find_all(name='ul', attrs={"class": "article-list"})
for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='li', attrs={"class": "article-list-item"})
    for item in arr:
        print(item.text.strip())


# 最新活动
url = "https://support.hbfile.net/hc/zh-cn/categories/360000031362-%E6%9C%80%E6%96%B0%E6%B4%BB%E5%8A%A8"
ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
soup = BeautifulSoup(ret.text, 'html.parser')
article_names = ["官方活动", "持仓奖励", "Prime", "FastTrack", "交易竞赛", "其他"]
article_list_arr = soup.find_all(name='ul', attrs={"class": "article-list"})
for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='li', attrs={"class": "article-list-item"})
    for item in arr:
        print(item.text.strip())
