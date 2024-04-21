# coding=utf-8
import json

import tumbler.config as config
from tumbler.api.rest import RestClient


class DingTalkAlertService(RestClient):

    def __init__(self, token):
        super(DingTalkAlertService, self).__init__()
        self.token = token
        self.init(config.SETTINGS["dingtalk_host"])

    def sign(self, request):
        """
        Generate HUOBI signature.
        """
        request.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 6.1; WOW64) \
            AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.71 Safari/537.36"
        }

        if request.method == "POST":
            request.headers["Content-Type"] = "application/json"

            if request.data:
                request.data = json.dumps(request.data)

        return request

    def send_msg(self, msg):
        msg = config.SETTINGS["dingtalk_keyword"] + ":" + msg
        params = {"access_token": self.token}
        data = {"msgtype": "text", "text": {"content": msg}}
        data = self.request("POST", "/robot/send", params=params, data=data)
        return data


ding_talk_service = DingTalkAlertService(config.SETTINGS["dingtalk_access_token"])
ding_talk_message_service = DingTalkAlertService(config.SETTINGS["dingtalk_message_access_token"])
ding_talk_friend_message_service = DingTalkAlertService(config.SETTINGS["dingtalk_friend_message"])
