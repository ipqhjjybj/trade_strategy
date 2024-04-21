# coding=utf-8

from collections import defaultdict
import time
from copy import copy

'''
rulers = {
    "/magnet/v3/merchant/list-orders": 5,
    "/mov/cancel_order": 5
}
for key, bef_times in rulers.items():

判断这个值是否在  bef_times 出现过，出现过则过滤
'''


class FilterTimes(object):

    def __init__(self, _rulers: dict):
        self.pre_times_dict = defaultdict(float)
        self.rulers = _rulers

    @staticmethod
    def get_url_key(method, path, params, data):
        return "method:{},path:{},params:{},data:{}".format(method, path, params, data)

    def is_filtered(self, key):
        """
        判断是否被过滤掉
        :param key: string
        :return: boolean
        """
        now = time.time()
        is_filtered = False
        for k, v in self.rulers.items():
            if k in key:
                if now - self.pre_times_dict[key] <= v:
                    is_filtered = True
                    break
                else:
                    self.pre_times_dict[key] = now
                    break

        all_items = copy(list(self.pre_times_dict.items()))
        for k, v in all_items:
            if now - v > 60:
                self.pre_times_dict.pop(k)
        return is_filtered
