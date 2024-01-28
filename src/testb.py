import time
import ubus
from threading import Thread

ubus.connect()

ubus.listen('test2', lambda v: print(v))

ubus.add('t1obj', {
    "get_v": lambda v: {'vv': "testb v"},
    'hello': lambda _: print('hello testb')
})

ubus.on_disconnect(lambda: exit())

def th_cb():
    while True:
        time.sleep(2)
        res = ubus.call('tobj', 'get_v')
        print(res)
        ubus.call('tobj', 'hello')

Thread(daemon=True, target=th_cb).start()

while True:
    time.sleep(1)
    ubus.send('test1', {"testdata": "test data b"})
    res = ubus.call('tobj', 'get_v')
    print(res)
    ubus.call('tobj', 'hello')
    ubus.call('tobj', 'param', {'data': 3213})
