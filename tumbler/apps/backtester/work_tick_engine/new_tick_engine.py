import sys
import numpy as np
import pandas as pd
# from qpython import *
import heapq


class InsConfig:
    def __init__(self):
        contracts = [
            {'symType': 'btc_usdt', 'commType': "value", 'commValue': 0.1, 'factor': 1}
        ]
        self.contractTable = pd.DataFrame(contracts)
        l = self.contractTable['symType'].tolist()
        self.contractTable['symType'] = l
        self.contractTable.set_index('symType', inplace=True)

    def setNoComm(self):
        self.contractTable.iloc[:, 1] = 'value'
        self.contractTable.iloc[:, 2] = 0

    def getContractFactor(self, symbol):
        return self.contractTable.loc[symbol]['factor']

    def generateCommFunc(self, symbol):
        entry = self.contractTable.loc[symbol]
        commT = entry['commType']
        commV = entry['commValue']
        cf = entry['factor']
        if commT == 'value':
            return lambda qty, price: qty * commV / cf
        elif commT == 'pct':
            return lambda qty, price: qty * price * commV * 0.01

    def generateSingleInsConfig(self, symbol):
        tbl = {'symbol': symbol}
        entry = self.contractTable.loc[symbol]
        tbl['commType'] = entry['commType']
        tbl['commValue'] = entry['commValue']
        tbl['factor'] = entry['factor']
        return tbl


class Order:
    def __init__(self, oid, symbol, side, openOrClose, price, qty):
        self.id = oid
        self.symbol = symbol
        self.side = side
        self.OpenOrClose = openOrClose
        self.price = price
        self.qty = qty
        self.aheadqty = None
        self.time = None

    def __str__(self):
        return 'id:{} symbol:{} price:{} aheadqty:{}'.format(self.id, self.symbol, self.price, self.aheadqty)

    def __gt__(self, other):
        if self.price - other.price < 1e-6:
            if self.side == 'S':
                return self.aheadqty > other.aheadqty
            elif self.side == 'B':
                return self.aheadqty < other.aheadqty
        else:
            return self.price > other.price

    def __lt__(self, other):
        if self.price - other.price < 1e-6:
            if self.side == 'S':
                return self.aheadqty < other.aheadqty
            elif self.side == 'B':
                return self.aheadqty > other.aheadqty
        else:
            return self.price < other.price


class OrderHeap(object):
    def __init__(self, type='ask'):
        self.type = type
        self.data = []

    def __len__(self):
        return len(self.data)

    def size(self):
        return self.__len__()

    def __getitem__(self, idx):
        return self.data[idx]

    def put(self, elem):
        heapq.heappush(self.data, elem)

    def get(self):
        if self.type == 'ask':
            return self.data[0]
        elif self.type == 'bid':
            return self.data[-1]

    def pop(self):
        if self.type == 'ask':
            return self.data.pop(0)
        elif self.type == 'bid':
            return self.data.pop(len(self.data) - 1)

    def onTrade(self, trd):
        ord = self.pop()
        assert trd.id == ord.id, "trade wrong"


class OrderNode:
    def __init__(self, Data=None):
        self.Data = None
        self.Next = None
        self.Next2 = None
        if Data != None:
            self.Data = Data

    def insert(self, newNode):
        newNode.Next = self.Next
        self.Next = newNode

    def insert2(self, newNode):
        newNode.Next2 = self.Next2
        self.Next2 = newNode

    def removeNext(self):
        delnode = self.Next
        assert not delnode.Next2, '[Error] level 2 node not empty.'
        self.Next = delnode.Next
        del delnode

    def removeNext2(self):
        delnode = self.Next2
        self.Next2 = delnode.Next2
        del delnode


class Trade:
    def __init__(self, id, symbol, side, qty, price, time, type):
        self.id = id
        self.symbol = symbol
        self.side = side
        self.qty = qty
        self.price = price
        self.time = time
        self.type = type

    def __str__(self):
        return '%s traded orderid:%d %s %d@%.2f' % (self.time, self.id, self.side, self.qty, self.price)


class Strategy:
    def __init__(self, recordFloatPnL=False):
        self.orderMap = {}
        self.pos = 0
        self.posMap = {}
        self.nCancel = 0
        self.pending = 0
        self.engine = None
        self.nav = 0
        self.cash = 0
        self.time_arr = None
        self.nav_arr = None
        self.comm_arr = None
        # self.signal_arr = None
        self.commission = 0
        self.commfunc = {}
        self.recordFloatPnL = recordFloatPnL
        if recordFloatPnL:
            self.floatPnLFile = open('floatPnL.csv', 'w')

    def OnInit(self, engine):
        self.engine = engine
        self.time_arr = np.empty((engine.ticks.shape[0], 1), dtype='datetime64[ms]')
        self.nav_arr = np.empty((engine.ticks.shape[0], 1))
        self.pos_arr = np.empty((engine.ticks.shape[0], 1))
        self.comm_arr = np.empty((engine.ticks.shape[0], 1))
        # self.signal_arr = np.empty((engine.ticks.shape[0], 1))

    def UpdateNav(self):
        self.nav = self.pos * self.engine.ctick['mid'] + self.cash
        self.time_arr[self.engine.i] = np.datetime64(self.ctime)
        self.nav_arr[self.engine.i] = self.nav
        self.pos_arr[self.engine.i] = self.pos
        self.comm_arr[self.engine.i] = self.commission
        # self.signal_arr[self.engine.i] = self.signal
        if self.recordFloatPnL:
            cprc = self.engine.ctick['price']
            for id, pos in self.posMap.items():
                if pos.side == 'B':
                    print('%d, %f' % (id, cprc - pos.price), file=self.floatPnLFile)
                else:
                    print('%d, %f' % (id, pos.price - cprc), file=self.floatPnLFile)

    def sendOrder(self, symbol, BorS, OorC, price, qty):
        o = self.engine.sendOrder(symbol, BorS, OorC, price, qty)
        self.orderMap[o.id] = o
        if BorS == 'B':
            self.pending += qty
        elif BorS == 'S':
            self.pending -= qty
        return o

    def cancelOrder(self, oid):
        o = self.FindOrder(oid)
        if o:
            self.engine.cancelOrder(oid)
            del self.orderMap[oid]
            self.nCancel += 1
            if o.side == 'B':
                self.pending -= o.qty
            elif o.side == 'S':
                self.pending += o.qty
        else:
            return

    def OnTrade(self, trd):
        if trd.symbol not in self.commfunc:
            self.commfunc[trd.symbol] = self.engine.insCfg.generateCommFunc(trd.symbol)
        if trd.side == 'B':
            if self.recordFloatPnL:
                if self.pos >= 0:  # open pos
                    self.posMap[trd.id] = trd
                else:
                    for id in list(self.posMap):
                        pos = self.posMap[id]
                        closeqty = min(pos.qty, trd.qty)
                        pos.qty -= closeqty
                        if pos.qty == 0:
                            del self.posMap[id]
            self.pos += trd.qty
            if trd.type == 'F':
                del self.orderMap[trd.id]
            self.pending -= trd.qty

            self.cash -= trd.price * trd.qty
            comm = self.commfunc[trd.symbol](trd.qty, trd.price)
            self.cash -= comm
            self.commission += comm
            # print('comm:%.2f' % self.commfunc[trd.symbol](trd.qty, trd.price))
            self.nav = self.pos * self.engine.ctick['mid'] + self.cash
        elif trd.side == 'S':
            if self.recordFloatPnL:
                if self.pos <= 0:
                    self.posMap[trd.id] = trd
                else:
                    for id in list(self.posMap):
                        pos = self.posMap[id]
                        closeqty = min(pos.qty, trd.qty)
                        pos.qty -= closeqty
                        if pos.qty == 0:
                            del self.posMap[id]
            self.pos -= trd.qty
            if trd.type == 'F':
                del self.orderMap[trd.id]
            self.pending += trd.qty

            self.cash += trd.price * trd.qty
            comm = self.commfunc[trd.symbol](trd.qty, trd.price)
            self.cash -= comm
            self.commission += comm
            # print('comm:%.2f' % self.commfunc[trd.symbol](trd.qty, trd.price))
            self.nav = self.pos * self.engine.ctick['mid'] + self.cash

    def OnFinish(self):
        if self.recordFloatPnL:
            self.floatPnLFile.close()

    def FindOrder(self, id):
        if id not in self.orderMap:
            print('wrong order id:%d' % id, file=sys.stderr)
            return None
        return self.orderMap[id]


class TickEngine:
    def __init__(self, fillParam=1.0, queueParam=1.0, debug=False, logfileName=''):
        self.fillparam = fillParam
        self.queueparam = queueParam
        self.debug = debug
        self.lastDay = -1
        self.nOrder = 0
        self.nAsk = 0
        self.nBid = 0
        self.oid = 0
        self.orderMap = {}
        self.allTrade = []
        self.strategy = None
        self.ticks = None
        self.pos = None
        self.BuyPrc = None
        self.SellPrc = None
        self.i = 0
        self.ctick = None
        self.askQueue = OrderNode()
        self.bidQueue = OrderNode()
        self.insCfg = InsConfig()

        if debug:
            assert (len(logfileName)) > 0, 'No log file name specified when using debug mode.'
            self.logfile = open(logfileName, 'a+')

    def RegisterStrategy(self, st):
        self.strategy = st

    def Start(self, ticks):
        self.ticks = ticks
        self.strategy.OnInit(self)
        self.pos = np.zeros((ticks.shape[0], 1))
        self.BuyPrc = np.zeros((ticks.shape[0], 1))
        self.SellPrc = np.zeros((ticks.shape[0], 1))

        columns = ticks.columns
        data = ticks.values

        for i in range(ticks.shape[0]):
            self.i = i
            t = data[i, :]
            tick = dict(zip(columns, t))
            self.ctick = tick
            # if self.debug and self.nOrder != 0:
            #    self.printTick(tick)
            self.DoMatch(tick)
            self.strategy.OnTick(tick)
            self.pos[i] = self.strategy.pos
            # if self.debug and self.nOrder != 0:
            #    self.printOrderBook()

        self.strategy.OnFinish()
        if self.debug:
            self.logfile.close()

    def printTick(self, tick):
        print('Quote|\t time:%s\nask:%.2f\t%d\nbid:%.2f\t%d' % (
            tick['date'], tick['ask1'], tick['askvol1'], tick['bid1'], tick['bidvol1']), file=self.logfile)

    def printOB(self, node):
        if node:
            node = node.Next
        while node:
            nextorder = node.Next2
            while nextorder:
                order = nextorder.Data
                self.logfile.write('%.2f %d | ' % (order.price, order.aheadqty))
                nextorder = nextorder.Next2
            print('\n', file=self.logfile)
            node = node.Next

    def printOrderBook(self):
        self.logfile.write('Order\nAsk: %d ' % self.nAsk)
        self.printOB(self.askQueue)
        self.logfile.write('Bid: %d ' % self.nBid)
        self.printOB(self.bidQueue)
        print('\n------------------------------------------', file=self.logfile)

    def getOB(self, node):
        obstr = ''
        if node:
            node = node.Next
        while node:
            nextorder = node.Next2
            while nextorder:
                order = nextorder.Data
                obstr += '%.2f %s | ' % (order.price, order.aheadqty)
                nextorder = nextorder.Next2
            obstr += '\n'
            node = node.Next
        return obstr

    def getOrderBookStr(self):
        return 'OrderBook\nAsk: %d %sBid: %d %s' \
               % (self.nAsk, self.getOB(self.askQueue), self.nBid, self.getOB(self.bidQueue))

    def sendOrder(self, symbol, BuyOrSell, OpenOrClose, price, vol):
        assert BuyOrSell in ['B', 'S'], 'Wrong Order Side.'
        assert OpenOrClose in ['O', 'C'], 'Wrong OpenOrClose flag.'
        assert price > 0.0, 'Order Price must be positive.'
        assert vol > 0.0, 'Order volumn must be positive.'

        self.oid += 1
        order = Order(self.oid, symbol, BuyOrSell, OpenOrClose, price, vol)
        assert self.ctick is not None, 'no market data before insert orders.'
        if BuyOrSell == 'B':
            if price == self.ctick['bid1']:
                order.aheadqty = self.ctick['bidvol1'] * self.queueparam
            else:
                order.aheadqty = np.nan  # None
        elif BuyOrSell == 'S':
            if price == self.ctick['ask1']:
                order.aheadqty = self.ctick['askvol1'] * self.queueparam
            else:
                order.aheadqty = np.nan  # None
        self.insertOrder(order)
        order.time = self.ctick['date']
        return order

    def insertOrder(self, order):
        price = order.price
        newOrderNode = OrderNode(order)
        if order.side == 'B':
            next = self.bidQueue
            while next.Next and price < next.Next.Data:
                next = next.Next
            if not next.Next or price > next.Next.Data:
                node = OrderNode(price)
                next.insert(node)
            next = next.Next
            nextorder = next
            while nextorder.Next2:
                nextorder = nextorder.Next2
            nextorder.insert2(newOrderNode)
            self.nBid += 1
        elif order.side == 'S':
            next = self.askQueue
            while next.Next and price > next.Next.Data:
                next = next.Next
            if not next.Next or price < next.Next.Data:
                node = OrderNode(price)
                next.insert(node)
            next = next.Next
            nextorder = next
            while nextorder.Next2:
                nextorder = nextorder.Next2
            nextorder.insert2(newOrderNode)
            self.nAsk += 1
        self.nOrder += 1
        self.orderMap[order.id] = order

    def removeOrder(self, node2, orderid, side):
        node2.removeNext2()
        del self.orderMap[orderid]

    def cancelOrder(self, orderid):
        isCanceled = False
        assert orderid in self.orderMap, 'No such Order id:%d' % orderid
        Order = self.orderMap[orderid]
        price = Order.price
        self.nOrder -= 1
        if Order.side == 'B':
            node = self.bidQueue
            self.nBid -= 1
        elif Order.side == 'S':
            node = self.askQueue
            self.nAsk -= 1
        assert node, 'Order Queue empty.'
        while node.Next and price != node.Next.Data:
            node = node.Next
        node2 = node.Next
        while node2.Next2 and orderid != node2.Next2.Data.id:
            node2 = node2.Next2
        if node2.Next2:
            self.removeOrder(node2, orderid, Order.side)
            isCanceled = True
        if not node.Next.Next2:
            node.removeNext()
        assert isCanceled, 'Order id:%d Not Found in Queue.' % orderid

    def DoTrade(self, trdprice, node2, order, msg):
        if self.debug:
            print('Trade\n%s %.2f' % (order.side, trdprice), file=self.logfile)
        trd = Trade(order.id, order.symbol, order.side, order.qty, trdprice, self.ctick['date'], 'F')
        trd.msg = msg
        self.strategy.OnTrade(trd)
        self.removeOrder(node2, order.id, order.side)
        self.allTrade.append(trd)
        if order.side == 'B':
            self.BuyPrc[self.i] = trdprice
            self.nBid -= 1
        elif order.side == 'S':
            self.SellPrc[self.i] = trdprice
            self.nAsk -= 1

    def DoMatch(self, tick):
        bid1 = tick['bid1']
        ask1 = tick['ask1']
        bidvol1 = tick['bidvol1']
        askvol1 = tick['askvol1']
        bidFill = tick['bidFill']
        askFill = tick['askFill']
        afPrice = tick['afPrice']
        bfPrice = tick['bfPrice']

        node = self.bidQueue
        while node and node.Next and bid1 <= node.Next.Data:
            node2 = node.Next
            price = node.Next.Data
            if bid1 - price < -1e-6:  # bid1 < price
                # 1、盘口买价小于买单价格，直接fill买单，DoTrade
                while node2 and node2.Next2:
                    order = node2.Next2.Data
                    trdprice = max(bid1, order.price)
                    self.DoTrade(trdprice, node2, order, '1')
            else:
                order = node2.Next2.Data
                if abs(price - bid1) < 1e-6:  # price == bid1
                    if np.isnan(order.aheadqty):
                        # 如果aheadqty为空补足
                        order.aheadqty = bidvol1 * self.queueparam
                    elif order.aheadqty >= bidvol1:
                        # 如果aheadqty >= bidvol1, 增加bidFill
                        bidFill = max(bidFill, self.fillparam * (order.aheadqty - bidvol1))

                # 2. bfprice小于等于买单价：
                #    1).每个同bfprice的买单aheadqty减去bidFill，如aheadqty == 0，DoTrade
                #    2).每个买单价大于bfprice的买单fill，DoTrade
                while node2 and node2.Next2:
                    order = node2.Next2.Data
                    if abs(price - bfPrice) < 1e-6:  # price == bfPrice
                        order.aheadqty -= bidFill
                        if order.aheadqty <= 0:
                            self.DoTrade(price, node2, order, '2')
                    elif price - bfPrice > 1e-6:  # price > bfPrice
                        self.DoTrade(price, node2, order, '3')
                    node2 = node2.Next2
            if not node.Next.Next2:
                node.removeNext()
            node = node.Next

        node = self.askQueue
        while node and node.Next and ask1 >= node.Next.Data:
            node2 = node.Next
            price = node.Next.Data
            if ask1 - price > 1e-6:  # ask1 > price
                while node2 and node2.Next2:
                    order = node2.Next2.Data
                    trdprice = min(ask1, order.price)  # // max -> min
                    self.DoTrade(trdprice, node2, order, '4')
            else:
                order = node2.Next2.Data
                if abs(price - ask1) < 1e-6:  # price == ask1
                    if np.isnan(order.aheadqty):
                        order.aheadqty = askvol1 * self.queueparam
                    elif order.aheadqty >= askvol1:
                        askFill = max(askFill, self.fillparam * (order.aheadqty - askvol1))

                while node2 and node2.Next2:
                    order = node2.Next2.Data
                    if abs(price - afPrice) < 1e-6:
                        order.aheadqty -= askFill
                        if order.aheadqty <= 0:
                            self.DoTrade(price, node2, order, '5')
                    elif price - afPrice < -1e-6:
                        self.DoTrade(price, node2, order, '6')
                    node2 = node2.Next2
            if not node.Next.Next2:
                node.removeNext()
            node = node.Next
