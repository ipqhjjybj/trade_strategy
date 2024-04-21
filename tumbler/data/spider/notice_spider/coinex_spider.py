# coding=utf-8

import requests
from urllib.parse import urlencode
from bs4 import BeautifulSoup


headers = {
    'Content-Type': 'application/json; charset=utf-8',
    'Accept': 'application/json',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
    'authority': 'announcement.coinex.com',
    'cookie': '_ga=GA1.2.1669655843.1572239240; __zlcmid=vAiffOaUvCkAy3; __cfruid=bb2ea4a5b292e6dcc193d66fd64b4d65ff071be1-1611191021; __cfduid=d1c149f0536742be1a36587e941b50bdd1612502419; _help_center_session=Rk1Bc0NMaVZpL1pKRExQa0xlTmx5bDZUbkZPNTdFKzJwN244RVhycWtWbDR1bmhlME9qTEc5Wjd5bTgxSmdqRUhJMUhDUHZkV281VTkvM1l6dElJZ3o1RGpraWtKRkFQY2J6VnhqeHdPb24xaDZXVGtGaGs2WW9OblhCS2J6WkYxeDdNRENXSkpScUtOTjI1TmRpa2VSbzVidS8yUDFUZDFDVEYxZHo3N084VzRsZ0hScWFXa0l1LzNsWGdPZnNLZmlYODh0T2N1azRWUlN2bVh3djhyWUdTN1FMUWV2V00wT3Q5bXo5cmg0QytYaHVlNm15cUZ4bVF3akY2d3RWaExMYlIvcjFaUkg2VXZBNnEzbFQrcUR0QlNKOURJQndvQXA3M0RjYzlFaXc9LS1EWUFDQ1hmemRFMjQxZUxuM1dkbklnPT0%3D--2bc9162ed6c8f3ee8d66cb152cb6bc27b596bed7; __cf_bm=8666b408277d00e443982ff3a2792a5b86fbe485-1613787786-1800-AY3/1KbAfT4nV54G5X/UB1bcowNjyR47Zb+49pN8xH36mN5ItjzyK62ug+YXcG7hUS1rDiONx0byDWvlY81yVbpiyuFWUg16qxyASPk3zAo1uhCMVOBr7WH+FmdRBR8I5A=='
}

url = "https://announcement.coinex.com/hc/zh-cn"

ret = requests.request("GET", url, headers=headers, params={}, data=None, timeout=15)
print(ret.text)
soup = BeautifulSoup(ret.text, 'html.parser')

article_names = ["新币上线", "重要公告", "服务升级", "充值提现", "CET专栏"]
article_list_arr = soup.find(name="div", attrs={"class": "custom-section"}).find_all(name="div", attrs={"class": "custom-tree"})
for article_name, article_list in zip(article_names, article_list_arr):
    print(article_name)
    arr = article_list.find_all(name='a')
    for item in arr:
        print(item.text.strip())


