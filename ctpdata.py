from datetime import datetime, timedelta

from backtrader.feed import DataBase
from backtrader import TimeFrame, date2num, num2date
from backtrader.utils.py3 import (integer_types, queue, string_types,
                                  with_metaclass)
from backtrader.metabase import MetaParams
from ctpstore import CTPStore


class MetaCTPData(DataBase.__class__):
    def __init__(cls, name ,bases, dct):
        super(MetaCTPData, cls).__init__(name, bases, dct)

        CTPStore.DataCls = cls


class CTPData(with_metaclass(MetaCTPData, DataBase)):
    params = (
        ('qcheck', 0.5),

    )

    _store = CTPStore

    _ST_FROM, _ST_START, _ST_LIVE, _ST_HISTORBACK, _ST_OVER = range(5)

    _TOFFSET = timedelta()

    def _timeoffset(self):
        return self._TOFFSET

    def islive(self):

        return True

    def __init__(self, **kwargs):
        self.c = self._store(**kwargs)
        self._candleFormat = 'bidask' if self.p.bidask else 'midpoint'

    def setenvironment(self, env):
        super(CTPData, self).setenvironment(env)
        env.addstore(self.c)

    def start(self):
        super(CTPData, self).start()

        # create attributes as soon as possible
        self._statelivereconn = False # if reconnectiong in live state
        self._storedmsg = dict() # keep pending live message (under None)
        self.qlive = queue.Queue()
        self._state = self._ST_OVER

        # kickstart store and get queue to wait on
        self.c.start(data=self)

        # check if the granularity is supported
        otf = self.c.get_granularity(self._timeframe, self._compression)
        if otf is None:
            self.put_notification(self.NOTSUPPORTED_TF)
            self._state = self._ST_OVER
            return

        self.contractdetails = cd = self.c.get_instrument(self.p.dataname)
        if cd is None:
            self.put_notification(self.NOTSUBSCRIBED)
            self._state = self._ST_OVER
            return

        if self.p.backfill_from is not None:
            self._state = self._ST_FROM
            self.p.backfill_from.start()
        else:
            self._start_finish()
            self._state = self._ST_START
            self._st_start()

        self.reconns = 0

    def _st_start(self, instart=True, tmout=None):
        if self.p.historical:
            self.put_notification(self.DELAYED)
            dtend = None
            if self.todate < float('inf'):
                dtend = num2date(self.todate)

            dtbegin = None
            if self.fromate > float('-inf'):
                dtbegin = num2date(self.fromdate)

            self.qhist = self.c.candles(
                self.p.dataname, dtbegin, dtend,
                self._timeframe, self._compression,
                candleFormat=self._candleFormat,
                includeFirst=self.p.includeFirst
            )

            self._state = self._ST_HISTORBACK
            return True

        self.qlive = self.c.streaming_prices(self.p.dataname, tmout=tmout)
        if instart:
            self._statelivereconn = self.p.backfill_start
        else:
            self._statelivereconn = self.p.backfill

        if self._statelivereconn:
            self.put_notification(self.DELAYED)

        self._state = self._ST_LIVE
        if instart:
            self._reconns = self.p.reconnections

        return True

    def stop(self):
        super(CTPData, self).stop()
        self.c.stop()

    def haslivedata(self):
        return bool(self._storedmsg or self.qlive)

    def _load(self):
        if self._state == self._ST_OVER:
            return False

        while True:
            if self._state == self._ST_LIVE:
                try:
                    msg = (self._storedmsg.pop(None, None) or
                           self.qlive.get(timeout=self._qcheck))
                except queue.Empty:
                    return None  # indicate timeout situation

                if msg is None:  # Conn broken during historical/backfilling
                    self.put_notification(self.CONNBROKEN)
                    # Try to reconnect
                    if not self.p.reconnect or self._reconns == 0:
                        # Can no longer reconnect
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # failed

                    self._reconns -= 1
                    self._st_start(instart=False, tmout=self.p.reconntimeout)
                    continue

                if 'code' in msg:
                    self.put_notification(self.CONNBROKEN)
                    code = msg['code']
                    if code not in [599, 598, 596]:
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # failed

                    if not self.p.reconnect or self._reconns == 0:
                        # Can no longer reconnect
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # failed

                    # Can reconnect
                    self._reconns -= 1
                    self._st_start(instart=False, tmout=self.p.reconntimeout)
                    continue

                self._reconns = self.p.reconnections

                # Process the message according to expected return type
                if not self._statelivereconn:
                    if self._laststatus != self.LIVE:
                        if self.qlive.qsize() <= 1:  # very short live queue
                            self.put_notification(self.LIVE)

                    ret = self._load_tick(msg)
                    if ret:
                        return True

                    # could not load bar ... go and get new one
                    continue

                # Fall through to processing reconnect - try to backfill
                self._storedmsg[None] = msg  # keep the msg

                # else do a backfill
                if self._laststatus != self.DELAYED:
                    self.put_notification(self.DELAYED)

                dtend = None
                if len(self) > 1:
                    # len == 1 ... forwarded for the 1st time
                    dtbegin = self.datetime.datetime(-1)
                elif self.fromdate > float('-inf'):
                    dtbegin = num2date(self.fromdate)
                else:  # 1st bar and no begin set
                    # passing None to fetch max possible in 1 request
                    dtbegin = None

                dtend = datetime.utcfromtimestamp(int(msg['time']) / 10 ** 6)

                self.qhist = self.o.candles(
                    self.p.dataname, dtbegin, dtend,
                    self._timeframe, self._compression,
                    candleFormat=self._candleFormat,
                    includeFirst=self.p.includeFirst)

                self._state = self._ST_HISTORBACK
                self._statelivereconn = False  # no longer in live
                continue

            elif self._state == self._ST_HISTORBACK:
                msg = self.qhist.get()
                if msg is None:  # Conn broken during historical/backfilling
                    # Situation not managed. Simply bail out
                    self.put_notification(self.DISCONNECTED)
                    self._state = self._ST_OVER
                    return False  # error management cancelled the queue

                elif 'code' in msg:  # Error
                    self.put_notification(self.NOTSUBSCRIBED)
                    self.put_notification(self.DISCONNECTED)
                    self._state = self._ST_OVER
                    return False

                if msg:
                    if self._load_history(msg):
                        return True  # loading worked

                    continue  # not loaded ... date may have been seen
                else:
                    # End of histdata
                    if self.p.historical:  # only historical
                        self.put_notification(self.DISCONNECTED)
                        self._state = self._ST_OVER
                        return False  # end of historical

                # Live is also wished - go for it
                self._state = self._ST_LIVE
                continue

            elif self._state == self._ST_FROM:
                if not self.p.backfill_from.next():
                    # additional data source is consumed
                    self._state = self._ST_START
                    continue

                # copy lines of the same name
                for alias in self.lines.getlinealiases():
                    lsrc = getattr(self.p.backfill_from.lines, alias)
                    ldst = getattr(self.lines, alias)

                    ldst[0] = lsrc[0]

                return True

            elif self._state == self._ST_START:
                if not self._st_start(instart=False):
                    self._state = self._ST_OVER
                    return False

    def _load_tick(self, msg):
        dtobj = datetime.utcfromtimestamp(int(msg['time']) / 10 ** 6)
        dt = date2num(dtobj)
        if dt <= self.lines.datetime[-1]:
            return False  # time already seen

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.volume[0] = 0.0
        self.lines.openinterest[0] = 0.0

        # Put the prices into the bar
        tick = float(msg['ask']) if self.p.useask else float(msg['bid'])
        self.lines.open[0] = tick
        self.lines.high[0] = tick
        self.lines.low[0] = tick
        self.lines.close[0] = tick
        self.lines.volume[0] = 0.0
        self.lines.openinterest[0] = 0.0

        return True

    def _load_history(self, msg):
        dtobj = datetime.utcfromtimestamp(int(msg['time']) / 10 ** 6)
        dt = date2num(dtobj)
        if dt <= self.lines.datetime[-1]:
            return False  # time already seen

        # Common fields
        self.lines.datetime[0] = dt
        self.lines.volume[0] = float(msg['volume'])
        self.lines.openinterest[0] = 0.0

        # Put the prices into the bar
        if self.p.bidask:
            if not self.p.useask:
                self.lines.open[0] = float(msg['openBid'])
                self.lines.high[0] = float(msg['highBid'])
                self.lines.low[0] = float(msg['lowBid'])
                self.lines.close[0] = float(msg['closeBid'])
            else:
                self.lines.open[0] = float(msg['openAsk'])
                self.lines.high[0] = float(msg['highAsk'])
                self.lines.low[0] = float(msg['lowAsk'])
                self.lines.close[0] = float(msg['closeAsk'])
        else:
            self.lines.open[0] = float(msg['openMid'])
            self.lines.high[0] = float(msg['highMid'])
            self.lines.low[0] = float(msg['lowMid'])
            self.lines.close[0] = float(msg['closeMid'])

        return True