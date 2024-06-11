# coding=utf-8

import time
import requests
from bs4 import BeautifulSoup

from tumbler.constant import Exchange
from tumbler.service import log_service_manager, ding_talk_message_service
from tumbler.function.os_class import read_all_lines, output_all_lines_to_file


class NoticeSpider(object):
    def __init__(self):
        self.store_path = "notice.csv"
        self.headers = {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': 'application/json',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36'
        }

        self.already_lines = set(self.load_from_file())
        self.run()

    @staticmethod
    def get_key(exchange, title, content):
        return ','.join([exchange, title, content])

    def load_from_file(self):
        """
        exchange, title, content
        """
        return read_all_lines(self.store_path)

    def basic_spider(self, exchange, url, article_names, func):
        ret = requests.request("GET", url, headers=self.headers, params={}, data=None, timeout=15)
        soup = BeautifulSoup(ret.text, 'html.parser')
        article_list_arr = func(soup)
        ret = []
        for article_name, article_list in zip(article_names, article_list_arr):
            arr = article_list.find_all(name='a')
            for item in arr:
                ret.append(NoticeSpider.get_key(exchange, article_name, item.text.strip()))
        return ret

    def binance_func(self, soup):
        return soup.find(name="div", attrs={"class": "css-1s5qj1n"}).find_all(name="div", attrs={"class": "css-6f91y1"})

    def huobi_func(self, soup):
        return soup.find_all(name='ul', attrs={"class": "article-list"})

    def okex_func(self, soup):
        return soup.find_all(name='ul', attrs={"class": "article-list"})

    def coinex_func(self, soup):
        return soup.find(name="div", attrs={"class": "custom-section"}).\
            find_all(name="div", attrs={"class": "custom-tree"})

    def gate_func(self, soup):
        return soup.find(name="div", attrs={"class": "help_submenu_content"})\
            .find_all(name="div", attrs={"class": "help_submenu_content_item"})

    def report_lines(self, lines):
        all_info = '\n'.join(lines)
        ding_talk_message_service.send_msg(all_info)

    def save_new_lines(self, lines):
        self.report_lines(lines)
        for line in lines:
            self.already_lines.add(line)
        output_all_lines_to_file(self.store_path, lines, is_first=False)

    def run(self):
        while True:
            try:
                for exchange, url, article_names, op_func in [
                    [
                        Exchange.BINANCE.value,
                        "https://www.binance.com/cn/support/announcement",
                        ["币币交易", "最新公告", "最新活动", "法币交易", "API公告"],
                        self.binance_func
                    ],
                    [
                        Exchange.HUOBI.value,
                        "https://support.hbfile.net/hc/zh-cn/categories/360000031902-%E9%87%8D%E8%A6%81%E5%85%AC%E5%91%8A",
                        ["最新热点", "HT专栏", "冲提公告", "新币上线", "项目介绍", "API公告", "其他"],
                        self.huobi_func
                    ],
                    [
                        Exchange.HUOBI.value,
                        "https://support.hbfile.net/hc/zh-cn/categories/360000031362-%E6%9C%80%E6%96%B0%E6%B4%BB%E5%8A%A8",
                        ["官方活动", "持仓奖励", "Prime", "FastTrack", "交易竞赛", "其他"],
                        self.huobi_func
                    ],
                    [
                        Exchange.HUOBI.value,
                        "https://support.hbfile.net/hc/zh-cn/categories/360000032161-%E5%90%88%E7%BA%A6%E4%BA%A4%E6%98%93",
                        ["USDT本位永续合约指引", "期权合约指引", "币本位永续合约指引", "币本位交割合约指引",
                         "条款说明", "重要公告", "API公告", "常见问题", "其他公告", "合约课堂", "合约产品导航"],
                        self.huobi_func
                    ],
                    [
                        Exchange.OKEX.value,
                        "https://www.okx.com/support/hc/zh-cn/categories/115000275131",
                        ["最新公告", "最新活动", "冲提公告", "法币公告", "币币/杠杆公告", "合约公告", "OKEX云", "矿池公告"],
                        self.okex_func
                    ],
                    [
                        Exchange.COINEX.value,
                        "https://announcement.coinex.com/hc/zh-cn",
                        ["新币上线", "重要公告", "服务升级", "充值提现", "CET专栏"],
                        self.coinex_func
                    ],
                    [
                        Exchange.GATEIO.value,
                        "https://www.gate.io/cn/help/annlist",
                        ["近期公告", "双周报", "上新", "理财", "最新活动", "直播", "交易大赛"],
                        self.gate_func
                    ],
                ]:
                    try:
                        lines = self.basic_spider(exchange, url, article_names, op_func)
                        new_lines = [line for line in lines if line not in self.already_lines]
                        if new_lines:
                            self.save_new_lines(new_lines)
                    except Exception as ex:
                        log_service_manager.write_log("[NoticeSpider] ex:{} exchange:{} url:{}"
                                                      .format(ex, ex, url))

            except Exception as ex:
                log_service_manager.write_log("[NoticeSpider] ex:{}".format(ex))
            time.sleep(10 * 60)


if __name__ == "__main__":
    an = NoticeSpider()
    an.run()
