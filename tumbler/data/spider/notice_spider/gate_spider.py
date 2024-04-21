# coding=utf-8

import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup

# ret = requests.get("https://support.hbfile.net/hc/zh-cn/categories/360000031902-%E9%87%8D%E8%A6%81%E5%85%AC%E5%91%8A")
# print(ret.text)

headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36'
}


url = "https://www.gate.io/cn/help/annlist"

ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["近期公告", "双周报", "上新", "理财", "最新活动", "直播", "交易大赛"]

article_list_arr = soup.find(name="div", attrs={"class": "help_submenu_content"}).find_all(name="div", attrs={"class": "help_submenu_content_item"})
for article_name, article_list in zip(article_names, article_list_arr):
    arr = article_list.find_all(name='a')
    for item in arr:
        print(item.text.strip())


