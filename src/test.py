import time
import ubus

ubus.connect()

ubus.listen('test1', lambda v: print(v))

ubus.add('tobj', {
    "get_v": lambda v: {'vv': 'test'},
    'hello': lambda _: print('hello test'),
    'param': lambda v: print(v)
})

ubus.on_disconnect(lambda: exit())
while True:
    time.sleep(1)
    ubus.send('test2', {"testdata": "test data"})
    res = ubus.call('t1obj', 'get_v')
    print(res)
    ubus.call('t1obj', 'hello')
