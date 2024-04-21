# coding:utf-8
import socket
import threading
import time
import os
import sys
import datetime
import chardet
import random
import pandas as pd
import numpy as np

import json
import requests

# url :http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=FPYA&fd=2015-12-31&st=2&sr=-1&p=1&ps=50&js=var%20bSSlzeeQ={pages:(pc),data:[(x)]}&stat=3&rt=48465217


DateToAchieve = "2015-12-31"


class Achievement(object):
    '''
    获取分配预告的数据
    http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=FPYA&fd=2015-12-31&st=2&sr=-1&p=1&ps=50&js=var%20bSSlzeeQ={pages:(pc),data:[(x)]}&stat=3&rt=48465217
    分配预告
    distribution forecast
    '''

    @staticmethod
    def downloadDistributionForecast(page, p="1", _date=DateToAchieve):
        try:
            page = str(page)
            p = str(p)
            url = 'http://datainterface.eastmoney.com/EM_DataCenter/JS.aspx?type=SR&sty=FPYA&fd=' + DateToAchieve + '&st=2&sr=-1&p=' + p + '&ps=' + page + '&js=var%20bSSlzeeQ={pages:(pc),data:[(x)]}&stat=3&rt=48465217'
            html = requests.get(url).text
            ret = []
            if len(html) > 13:
                html = html[13:]
                html = html.replace('pages', '"pages"').replace('data', '"data"')
                data = json.loads(html)
                data = data["data"]
                for dic in data:
                    (code, stock_name, ads, date, date_belong) = dic.split(',')
                    gaosongzhuan = 0
                    if "转" in ads or "送" in ads:
                        gaosongzhuan = 1
                    ret.append(
                        {
                            "code": code,
                            "stock_name": stock_name,
                            "ads": ads,
                            "date": date,
                            "date_belong": date_belong,
                            "gaosongzhuan": str(gaosongzhuan)
                        }
                    )
            return ret

        except Exception as ex:
            print(ex)
        return []


'''
上传分配预告的数据
'''
# @staticmethod
# def uploadDistributionForecast(page,rate = 50, _date=DateToAchieve):
# 	rate = int(rate)
# 	p = (int(page) + rate - 1) / rate
# 	# db.execute('delete from achievement_stock')
# 	# db.commit()
# 	for i in range(1, p + 1):
# 		print "i="+str(i)
# 		ret = Achievement.downloadDistributionForecast(rate,i,_date)
# 		for info in ret:
# 			code = info["code"]
# 			code = Stock.makeCode(code)
# 			stock_name = info["stock_name"]
# 			ads = info["ads"]
# 			date = info["date"]
# 			date_belong = info["date_belong"]
# 			gaosongzhuan = info["gaosongzhuan"]
# 			try:
# 				sqll = "insert into ACHIEVEMENT_STOCK(CODE,NAME,ADS,CAL_TIME,DATE_BELONG,GAOSONGZHUAN) values('%s','%s','%s','%s','%s','%s')" % (code, stock_name , ads , date , date_belong , gaosongzhuan )
# 				print(sqll)
# 				db.execute(sqll)
# 			except Exception as ex:
# 				print(ex)
# 		db.commit()


'''
日常
'''

# def Achievement_daily():
#     global DateToAchieve
#     for d in ["2015-12-31", "2016-03-31", "2016-06-30"]:
#         DateToAchieve = d
#         Achievement.uploadDistributionForecast(50, 50)


'''
主要main
'''

# def Achievement_main():
#     global DateToAchieve
#     DateToAchieve = '2015-06-30'
#     Achievement.uploadDistributionForecast(1000, 50)
#

if __name__ == '__main__':
    # ["2015-12-31", "2016-03-31", "2016-06-30"]
    print(Achievement.downloadDistributionForecast(1, 50, "2020-12-31"))
