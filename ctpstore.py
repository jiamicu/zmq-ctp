import collections
from datetime import datetime, timedelta
import time as time
import json
import threading

import zmq

import backtrader as bt
from backtrader.metabase import MetaParams
from backtrader.utils.py3 import queue, with_metaclass
from backtrader.utils import AutoDict

class API(object):
    """
    处理与zmq的连接与提供收发消息的接口
    """
    def __init__(self):
        '''Init zmq connect'''
        pass

    def run(self):
        pass

    def send(self):
        pass

    def on_success(self):
        pass

    def on_error(self):
        pass


class MetaSingleton(MetaParams):
    """Metaclass to make a metaclassed class a singleton"""
    def __init__(cls, name ,bases, dct):
        super(MetaSingleton, cls).__init__(name, bases, dct)
        cls._singleton = None

    def __call__(cls, *args, **kwargs):
        if cls._singleton is None:
            cls._singleton = (
                super(MetaSingleton, cls).__call__(*args, **kwargs))

        return cls._singleton


class CTPStore(with_metaclass(MetaSingleton, object)):
    """singleton class wrapping to control the connections to ctp

    Params:

        - ``practice`` (default: ``True``): use the test environment

        - ``account_tmout`` (default: ``10.0``): refresh period for account
            value/cash refresh
    """
    BrokerCls = None
    DataCls = None

    params = (
        ('practice', True),
        ('account_tmout', 10.0),
    )

    _DTEPOCH = datetime(1970, 1, 1)
    _ENVPRACTICE = 'practice'
    _ENVLIVE = 'live'

    @classmethod
    def getdata(cls, *args, **kwargs):
        return cls.DataCls(*args, **kwargs)

    @classmethod
    def getbroker(cls, *args, **kwargs):
        return cls.BrokerCls(*args, **kwargs)

    def __init__(self):
        super(CTPStore, self).__init__()

        self.notifs = collections.deque()  # store notifications for cerebro

        self._env = None  # reference to cerebro for general notifications
        self.broker = None # broker instance
        self.datas = list()  # datas that have registered over start

        self._oreders = collections.OrderedDict()  # map oreder.ref to oid
        self._oredersrev = collections.OrderedDict()  # map oid to order.ref
        self._transpend = collections.defaultdict(collections.deque)

        self._cenv = self._ENVPRACTICE if self.p.practice else self._ENVLIVE
        self.capi = API(environment=self._cenv)

        self._cash = 0.0
        self._value = 0.0
        self._env_acct = threading.Event()

    def start(self, data=None, broker=None):
        # Datas require some processing to kickstart data reception
        if data is None and broker is None:
            self.cash = None
            return

        if data is not None:
            self._env = data._env
            # for datas simulate a queue with None to kickstart co
            self.datas.append(data)

            if self.broker is not None:
                self.broker.data_started(data)

        elif broker is not None:
            self.broker = broker
            self.streaming_events()
            self.broker_threads()

    def stop(self):
        # signal end of thread
        if self.broker is not None:
            self.q_ordercreate.put(None)
            self.q_orderclose.put(None)
            self.q_account.put(None)

    def put_notification(self, msg, *args, **kwargs):
        self.notifs.append((msg, args, kwargs))

    def get_notifications(self):
        self.notifs.append(None)  # put a mark / threads could still append
        return [x for x in iter(self.notifs.popleft, None)]

    def get_positions(self):
        pass

    def get_granularity(self):
        pass

    def get_instrument(self):
        pass

    def streaming_events(self, tmout=None):
        q = queue.Queue()
        kwargs = {'q': q, 'tmout': tmout}

        t = threading.Thread(target=self._t_streaming_listener, kwargs=kwargs)
        t.daemon = True
        t.start()

        t = threading.Thread(target=self._t_streaming_events, kwargs=kwargs)
        t.daemon = True
        t.start()

        return q

    def _t_streaming_listener(self, q, tmout=None):
        while True:
            trans = q.get()
            self._transaction(trans)

    def _t_streaming_events(self, q, tmout=None):
        if tmout is not None:
            time.sleep(tmout)

    def candles(self, dataname, dtbegin, dtend, timeframe, compression,
                candleFormat, includeFirst):

        kwargs = locals().copy()
        kwargs.pop('self')
        kwargs['q'] = q = queue.Queue()
        t = threading.Thread(target=self._t_candles, kwargs=kwargs)
        t.daemon = True
        t.start()
        return q

    def _t_candles(self, dataname, dtbegin, dtend, timeframe, compression,
                candleFormat, includeFirst):

        granularity = self.get_granularity(timeframe, compression)
        if granularity is None:
            # e = error()
            # q.put(e.error_response)
            return

        dtkwargs = {}
        if dtbegin is not None:
            dtkwargs['start'] = int((dtbegin - self._DTEPOCH).total_seconds())

    def get_cash(self):
        return self._cash

    def get_value(self):
        return self._value

    _ORDEREXECS = {
        bt.Order.Market: 'market',
        bt.Order.Limit: 'limit',
        bt.Order.Stop: 'stop',
        bt.Order.StopLimit: 'stop',
    }

    def broker_threads(self):
        self.q_account = queue.Queue()
        self.q_account.put(True)
        t = threading.Thread(target=self._t_account)
        t.daemon = True
        t.start()

        self.q_ordercreate = queue.Queue()
        t = threading.Thread(target=self._t_order_create)
        t.daemon = True
        t.start()

        self.q_orderclose = queue.Queue
        t = threading.Thread(target=self._t_order_cancel)
        t.daemon = True
        t.start()

        # wait once for the values to be set
        self._evt_acct.wait(self.p.account_tmout)
