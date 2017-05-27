#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
__title__ = 'ctp with zmq'
__author__ = 'jiamicu'
__mtime__ = '2017/05/25'
"""

import _thread
import logging
import zmq

from adapters.ctp_quote import *
from adapters.ctp_trade import *

logger = logging.getLogger('ctpProxy')


class CTPProxy(object):

    def __init__(self, investor='088884', pwd='j649089992'):
        self.q = None
        self.t = None
        self.Investor = investor
        self.PassWord = pwd
        self.init_ctp()
        self.RelogEnable = True

        self.bk_state = 0

    def init_ctp(self):
        # if md api or td api exits , release
        if self.q:
            self.q.Release()
        if self.t:
            self.t.Release()

        self.q = CtpQuote()
        self.q.OnFrontConnected = self.q_OnFrontConnected
        self.q.OnRspUserLogin = self.q_OnRspUserLogin
        self.q.OnRspSubMarketData = self.OnRspSubMarketData
        self.q.OnRtnTick = self.q_Tick

        self.t = CtpTrade()
        self.t.OnFrontConnected = self.OnFrontConnected
        self.t.OnRspUserLogin = self.OnRspUserLogin
        self.t.OnRtnOrder = self.OnRtnOrder
        self.t.OnRtnCancel = self.OnRtnCancel
        self.t.OnRtnTrade = self.OnRtnTrade
        self.t.OnRtnInstrumentStatus = self.OnRtnInstrumentStatus
        self.t.OnRtnErrOrder = self.OnRtnErrOrder

    def io_emit(self, rsp_type_str, data):

        msg = dict(
            type=rsp_type_str,
            msg=data,
        )
        if self.bk_state == 0:
            self.bk_s.send_json(msg)
            self.bk_state = 1
            # self.rb_s.send_json(msg)
        print(str(msg['msg']))

    def start_ctp_server(self):
        self.context = zmq.Context(1)

        self.bk_s = self.context.socket(zmq.REP)
        self.md_s = self.context.socket(zmq.PUB)
        self.rb_s = self.context.socket(zmq.REP)

        self.bk_s.bind("tcp://*:8888")
        self.md_s.bind("tcp://*:9999")
        self.rb_s.bind("tcp://*:7777")

        poller = zmq.Poller()
        poller.register(self.bk_s, zmq.POLLIN)
        poller.register(self.rb_s, zmq.POLLIN)

        while True:
            socks = dict(poller.poll())

            if socks.get(self.bk_s) == zmq.POLLIN:
                message = self.bk_s.recv_json()
                self.bk_state = 0
                print("Received request: ", message)
                self.handle_msg(message)

            if socks.get(self.rb_s) == zmq.POLLIN:
                message = self.bk_s.recv_json()
                print("Received request: ", message)
                self.handle_msg(message)

    def stop_ctp_server(self):
        self.bk_s.close()
        self.md_s.close()
        self.rb_s.close()
        self.context.term()

    def handle_msg(self, message):
        """
        :param message: 
        :return: sen_json()
        json example:
        {
            type:  'the type of msg',
            msg:   'the content of msg',
        }
        """
        if message["type"] == "rsp_login":
            # self.init_ctp()
            self.Run()

        if message["type"] == "rsp_submk":
            self.p.ReqSubscribeMarketData(pInstrument=message["msg"])

    def OnRspSubMarketData(self):
        self.io_emit('rsp_submk', 'sub success')

    def OnFrontConnected(self):
        logger.info('{0} ctp connected'.format(self.Investor))
        if not self.RelogEnable:
            return
        self.t.ReqUserLogin(self.Investor, self.PassWord, '9999')

    def OnRspUserLogin(self, info=InfoField):
        """"""
        logger.info(info)
        print(info)
        if info.ErrorID == 0:
            # self.io_emit('rsp_login', 't login success')
            self.q.ReqConnect('tcp://180.168.212.228:41213')
        else:
            if info.ErrorID == 7:
                logger.info('ctp relogin')
                _thread.start_new_thread(self.relogin, ())
            else:
                self.io_emit('rsp_login', info.__dict__)
                self.t.OnFrontConnected = None
                self.RelogEnable = False
                _thread.start_new_thread(
                    self.release, ())  # 须放在thread中，否则无法释放资源

    def relogin(self):
        self.init_ctp()
        sleep(60 * 5)
        logger.info('relogin')
        self.Run()

    def release(self):
        self.t.Release()

    # Onrspuserlogin中调用
    def OnData(self):
        stats = 1
        while self.t.IsLogin and stats > 0:  # 没有交易的品种时不再循环发送::隔夜重启后会停止发送
            sleep(1)
            stats = sum(1 for n in self.t.DicInstrumentStatus.values()
                        if n == InstrumentStatusType.Continous)
            # logger.info(self.t.Account.__dict__)
            self.io_emit('rsp_account', self.t.Account.__dict__)
            # 需要在struct中增加obj2json的转换函数
            rtn = []
            for p in self.t.DicPositionField:
                rtn.append(self.t.DicPositionField[p].__dict__)
            self.io_emit('rsp_position', rtn)

    def OnRtnOrder(self, f=OrderField):
        """"""
        self.io_emit('rtn_order', f.__dict__)

    def OnRtnTrade(self, f=TradeField):
        """"""
        self.io_emit('rtn_trade', f.__dict__)

    def OnRtnCancel(self, f=OrderField):
        """"""
        self.io_emit('rtn_cancel', f.__dict__)

    def OnRtnErrOrder(self, f=OrderField, info=InfoField):
        """"""
        self.io_emit(
            'rtn_err_order', {
                'order': f.__dict__, 'info': info.__dict__})

    def q_OnFrontConnected(self):
        """"""
        self.q.ReqUserLogin(self.Investor, self.PassWord, '9999')

    def OnRtnInstrumentStatus(self, inst, status):
        # logger.info('{0}:{1}'.format(inst, status))
        pass

    # ----------------------------------------------------------------------
    def q_OnRspUserLogin(self, info=InfoField):
        """"""
        logger.info('quote' + info.__str__())
        self.io_emit('rsp_login', info.__dict__)

        self.io_emit('rsp_account', self.t.Account.__dict__)

        rtn = []
        for p in self.t.DicInstrument:
            rtn.append(self.t.DicInstrument[p].__dict__)
            self.io_emit('rsp_instrument', rtn)

        rtn = []
        for p in self.t.DicPositionField:
            rtn.append(self.t.DicPositionField[p].__dict__)
            self.io_emit('rsp_position', rtn)

        for p in self.t.DicOrderField:
            self.io_emit('rtn_order', self.t.DicOrderField[p].__dict__)
        for p in self.t.DicTradeField:
            self.io_emit('rtn_trade', self.t.DicTradeField[p].__dict__)
        # 开启循环发送权益与持仓
        _thread.start_new_thread(self.OnData, ())

    def q_Tick(self, field=Tick):
        """
        zmq回传tick行情
        :param field: 
        :return: 
        """
        md = dict(
            type = 'rtn_tick',
            msg = field.__dict__,
        )
        self.md_s.send_json(md)

    def Run(self):
        """"""
        self.RelogEnable = True
        self.t.ReqConnect('tcp://180.168.146.187:10000')

if __name__ == '__main__':
    ctp = CTPProxy()
    ctp.start_ctp_server()