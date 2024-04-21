"""
Event-driven framework of vn.py framework.
"""

from collections import defaultdict
from queue import Empty, Queue
from threading import Thread
from time import sleep

from tumbler.service.log_service import log_service_manager

"""
event_engine 推送的如 EVENT_TRADE, EVENT_ORDER, 
有时会构造  EVENT_ORDER + symbol的 topic话题来方便后面的一些驱动进行订阅
有加.的， 表示有这个可能
没加.的， 表示基本没这个可能
"""
EVENT_AUCTION_TIMER = "eAuctionTimer"
EVENT_AUCTION = "eAuction"
EVENT_TIMER = "eTimer"
EVENT_TICK = "eTick."
EVENT_MARKET_TRADE = "Tick.MarketTrade."
EVENT_TICK_REST = "eTick.Rest."
EVENT_TICK_WS = "eTick.Ws."
EVENT_SPREAD = "eSpread."
EVENT_BBO_TICK = "eBBO.Ticker."
EVENT_MERGE_TICK = "eTick.Merge."
EVENT_TRANSFER_TRADE = "eTrade.Transfer"
EVENT_TRADE = "eTrade."
EVENT_SENDER_ORDER = "eSendOrder."
EVENT_ORDER = "eOrder."
EVENT_COVER_ORDER_REQ = "eCoverOrderReq."
EVENT_REJECT_COVER_ORDER_REQ = "eRejectCoverOrderReq."
EVENT_POSITION = "ePosition."
EVENT_ACCOUNT = "eAccount."
EVENT_DICT_ACCOUNT = "eDictAccount."
EVENT_CONTRACT = "eContract."
EVENT_LOG = "eLog"
EVENT_TRANSFER = "eTransfer"
EVENT_TRADE_LOG = "eLog.Trade."                     # 导出 Trade 日志对象
EVENT_STRATEGY_VARIABLES_LOG = "eLog.Strategy."     # 导出 策略 运行时日志


class Event:
    """
    Event object consists of a type string which is used
    by event engine for distributing event, and a data
    object which contains the real data.
    """

    def __init__(self, e_type, data=None):
        self.e_type = e_type
        self.data = data


# Defines handler function to be used in event engine.
# HandlerType = Callable[[Event], None]


class EventEngine:
    """
    Event engine distributes event object based on its type
    to those handlers registered.

    It also generates timer event by every interval seconds,
    which can be used for timing purpose.
    """

    def __init__(self, interval=1):
        """
        Timer event is generated every 1 second by default, if
        interval not specified.
        """
        self._interval = interval
        self._queue = Queue()
        self._active = False
        self._thread = Thread(target=self._run)
        self._timer = Thread(target=self._run_timer)
        self._handler_dic_list = defaultdict(list)
        self._general_handlers = []

    def _run(self):
        """
        Get event from queue and then process it.
        """
        while self._active:
            try:
                event = self._queue.get(block=True, timeout=1)
                self._process(event)
            except Empty:
                pass
            except Exception as ex:
                log_service_manager.write_log("[event] _run error:{}".format(ex))

    def _process(self, event):
        """
        First distribute event to those handlers registered listening
        to this type.

        Then distribute event to those general handlers which listens
        to all types.
        """
        try:
            if event.e_type in self._handler_dic_list:
                [handler(event) for handler in self._handler_dic_list[event.e_type]]

            if self._general_handlers:
                [handler(event) for handler in self._general_handlers]
        except Exception as ex:
            log_service_manager.write_log("[event] _process error:{}".format(ex))

    def _run_timer(self):
        """
        Sleep by interval second(s) and then generate a timer event.
        """
        while self._active:
            sleep(self._interval)
            event = Event(EVENT_TIMER)
            self.put(event)

    def start(self):
        """
        Start event engine to process events and generate timer events.
        """
        if not self._active:
            self._active = True
            self._thread.start()
            self._timer.start()

    def stop(self):
        """
        Stop event engine.
        """
        self._active = False
        self._timer.join()
        self._thread.join()

    def put(self, event):
        """
        Put an event object into event queue.
        """
        self._queue.put(event)

    def register(self, e_type, handler):
        """
        Register a new handler function for a specific event type. Every
        function can only be registered once for each event type.
        """
        handler_list = self._handler_dic_list[e_type]
        if handler not in handler_list:
            handler_list.append(handler)

    def unregister(self, e_type, handler):
        """
        Unregister an existing handler function from event engine.
        """
        handler_list = self._handler_dic_list[e_type]

        if handler in handler_list:
            handler_list.remove(handler)

        if not handler_list:
            self._handler_dic_list.pop(e_type)

    def register_general(self, handler):
        """
        Register a new handler function for all event types. Every
        function can only be registered once for each event type.
        """
        if handler not in self._general_handlers:
            self._general_handlers.append(handler)

    def unregister_general(self, handler):
        """
        Unregister an existing general handler function.
        """
        if handler in self._general_handlers:
            self._general_handlers.remove(handler)
