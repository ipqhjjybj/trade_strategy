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
# 合约交易
url = "https://support.hbfile.net/hc/zh-cn/categories/360000032161-%E5%90%88%E7%BA%A6%E4%BA%A4%E6%98%93"
ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["USDT本位永续合约指引", "期权合约指引", "币本位永续合约指引", "币本位交割合约指引",
                 "条款说明", "重要公告", "API公告", "常见问题", "其他公告", "合约课堂", "合约产品导航"]
article_list_arr = soup.find_all(name='ul', attrs={"class": "article-list"})
for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='li', attrs={"class": "article-list-item"})
    for item in arr:
        print(item.text.strip())



