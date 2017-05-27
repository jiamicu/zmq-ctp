import sys
import time
import zmq

context = zmq.Context()

# socket to receive messages on
receiver_md = context.socket(zmq.SUB)
receiver_md.connect("tcp://localhost:7777")

# socket to send messages to
sender = context.socket(zmq.REQ)
sender.connect("tcp://localhost:8888")

poller = zmq.Poller()
poller.register(receiver_md, zmq.POLLIN)
poller.register(sender, zmq.POLLIN)
# process messages from both

msg = dict(
    type='rsp_login',
    msg='',
)

sender.send_json(msg)
while True:
    socks = dict(poller.poll())
    if socks.get(sender) == zmq.POLLIN:
        message = sender.recv_json()
        if message['msg'] == 'login success':
            sender.send_json(msg = dict(
                type='rsp_submk',
                msg='rb1708',
            ))
        else:print("recv:---"+str(message['msg']))
    if socks.get(receiver_md) == zmq.POLLIN:
        message = receiver_md.recv_json()
        print("md----"+str(message['msg']))
