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

url = "https://www.binance.com/cn/support/announcement"
ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["币币交易", "最新公告", "最新活动", "法币交易", "API公告"]
article_list_arr = soup.find(name="div", attrs={"class": "css-1s5qj1n"}).find_all(name="div", attrs={"class": "css-6f91y1"})

for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='a')
    for item in arr:
        print(item.text.strip())


# 最新公告
# url = "https://www.binance.com/cn/support/announcement/c-49"
# ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
# soup = BeautifulSoup(ret.text, 'html.parser')
# arr = soup.find(name="div", attrs={"class": "css-6f91y1"}).find(name='div', attrs={"class": "css-vurnku"})\
#     .find_all(name="a", attrs={"class": "css-1neg3js"})
# for t in arr:
#     print(t.text)

# 币币交易
# url = "https://www.binance.com/cn/support/announcement/c-48"
# ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
# soup = BeautifulSoup(ret.text, 'html.parser')
# arr = soup.find(name="div", attrs={"class": "css-6f91y1"}).find(name='div', attrs={"class": "css-vurnku"})\
#     .find_all(name="a", attrs={"class": "css-1neg3js"})
# for t in arr:
#     print(t.text)

# API交易
# url = "https://www.binance.com/cn/support/announcement/c-51"
# ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
# soup = BeautifulSoup(ret.text, 'html.parser')
# arr = soup.find(name="div", attrs={"class": "css-6f91y1"}).find(name='div', attrs={"class": "css-vurnku"})\
#     .find_all(name="a", attrs={"class": "css-1neg3js"})
# for t in arr:
#     print(t.text)

# 最新活动
# url = "https://www.binance.com/cn/support/announcement/c-93"
# ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
# soup = BeautifulSoup(ret.text, 'html.parser')
# arr = soup.find(name="div", attrs={"class": "css-6f91y1"}).find(name='div', attrs={"class": "css-vurnku"})\
#     .find_all(name="a", attrs={"class": "css-1neg3js"})
# for t in arr:
#     print(t.text)


