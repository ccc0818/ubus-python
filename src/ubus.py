import json
import socket
import logging
from threading import Thread, Lock, Event
from typing import Callable, Union
from uuid import uuid4
import select

_sock: socket.socket = None
_sock_lock = Lock()
_exit = False
_event_map: dict[str, set] = {}
_event_map_lock = Lock()
_request_map: dict[str, dict] = {}
_request_map_lock = Lock()
_objects_map: dict[str, dict[str, Callable[[dict], Union[None, dict]]]] = {}
_objects_map_lock = Lock()
_disconnect_cb = None

def i2b(i: int) -> bytes:
    return i.to_bytes(4, 'little')

def b2i(b: bytes) -> int:
    return int.from_bytes(b, 'little')

def _send(type: bytes, data: dict):
    global _sock_lock, _sock
    data = json.dumps(data).encode()
    data = type + data
    datalen = i2b(len(data))
    with _sock_lock:
        _sock and _sock.send(datalen + data)

def _request(id: str, type: bytes, data: dict, timeout=15):
    global _request_map_lock, _request_map
    _send(type, data)

    with _request_map_lock:
        _request_map[id] = {'event': Event()}

    if not _request_map[id]['event'].wait(timeout):
        logging.warn(f'request {data} timeout !')
        with _request_map_lock:
            del _request_map[id]
        return None

    res = _request_map[id]['data']
    with _request_map_lock:
        del _request_map[id]

    return res or None

def _msg_handler_thread():
    global _exit, _sock, _request_map, _request_map_lock, _event_map, _event_map_lock, _objects_map_lock, _objects_map, _disconnect_cb
    while not _exit:
        try:
            select.select([_sock], [], [])
            msg = _sock.recv(b2i(_sock.recv(4, socket.MSG_WAITALL)), socket.MSG_WAITALL)
            if len(msg) == 0:
                logging.critical('ubusd server disconnect !')
                _sock.close()
                _sock = None
                _exit = True
                _disconnect_cb and _disconnect_cb()
                return

            t = msg[0]
            data = json.loads(msg[1:])
            if t == 0xf2:
                with _objects_map_lock:
                    cb = _objects_map[data['object']][data['func']]
                res = cb(data['data']) if callable(cb) else None
                _send(b'\x02', {
                    '_id': data['_id'],
                    '_cs': data['_cs'],
                    'data': res
                })
            elif t == 0xf1:
                with _request_map_lock:
                    if data['ret']:
                        _request_map[data['_id']]['data'] = data['data']
                    else:
                        _request_map[data['_id']]['data'] = data['ret']
                    _request_map[data['_id']]['event'].set()
            elif t == 0xf3 or t == 0xf0:
                with _request_map_lock:
                    _request_map[data['_id']]['data'] = data['ret']
                    _request_map[data['_id']]['event'].set()
            elif t == 0xf4:
                with _event_map_lock:
                    event = data['event']
                    for cb in _event_map[event]:
                        callable(cb) and cb(data['data'])
        except Exception as e:
            logging.error(e)

def connect() -> bool:
    global _sock, _sock_lock
    with _sock_lock:
        try:
            _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.connect('/var/tmp/bus.sock')
            Thread(daemon=True, target=_msg_handler_thread).start()
        except Exception:
            _sock.close()
            _sock = None
            return False
        return True

def disconnect():
    global _sock, _sock_lock, _exit, _disconnect_cb
    with _sock_lock:
        if _sock:
            _sock.close()
            _sock = None
        _exit = True
        _disconnect_cb and _disconnect_cb()

def send(event: str, data: dict = {}):
    if not isinstance(event, str) or event == '':
        logging.error('parameter event must be a non-empty string !')
        return

    if not isinstance(data, dict):
        logging.error('parameter data must be a dict !')
        return

    msg = {
        'event': event,
        'data': data
    }
    _send(b'\x04', msg)

def listen(event: str, cb: Callable[[dict], any]) -> bool:
    global _event_map_lock, _event_map

    if not isinstance(event, str) or event == '':
        logging.error('parameter event must be a non-empty string !')
        return False

    if not callable(cb):
        logging.error('parameter cb must be a function !')
        return False

    id = str(uuid4())
    msg = {
        '_id': id,
        'event': event
    }

    res = _request(id, b'\x03', msg)

    if res:
        with _event_map_lock:
            if event in _event_map:
                _event_map[event].add(cb)
            else:
                _event_map[event] = {cb}

    return bool(res)

def add(object: str, methods: dict[str, Callable[[dict], Union[dict, None]]]) -> bool:
    global _objects_map, _objects_map_lock

    if not isinstance(object, str) or object == '':
        logging.error('parameter object must be a non-empty string !')
        return False

    if not isinstance(methods, dict) or not methods:
        logging.error('parameter methods must be a non-empty dict !')
        return False

    id = str(uuid4())
    msg = {
        '_id': id,
        'object': object,
        'funcs': list(methods.keys())
    }

    res = _request(id, b'\x00', msg)
    if res:
        with _objects_map_lock:
            _objects_map[object] = methods

    return bool(res)

def call(object: str, func: str, data: dict = {}) -> Union[None, dict]:
    if not isinstance(object, str) or object == '':
        logging.error('parameter object must be a non-empty string !')
        return

    if not isinstance(func, str) or func == '':
        logging.error('parameter func must be a non-empty string !')
        return

    if not isinstance(data, dict):
        logging.error('parameter data must be a dict !')
        return

    id = str(uuid4())
    msg = {
        '_id': id,
        'object': object,
        'func': func,
        'data': data
    }

    return _request(id, b'\x01', msg)

def on_disconnect(cb: Callable[[any], any]):
    global _disconnect_cb

    if not callable(cb):
        logging.error('parameter cb must be a function !')
        return
    _disconnect_cb = cb


if __name__ == "__main__":
    from sys import argv, exit
    import time
    import atexit

    def usage():
        print('usage: python bus.py [option]')
        print('option:')
        print('    call [object] [func] <args>: invoke the func of object')
        print("    listen [event]: listen the event")
        print("    send [event] <args>: send event")

    if len(argv) <= 1 or argv[1] == '-h' or argv[1] == '--help':
        usage()
        exit()

    if not connect():
        print('connect failed!')
        disconnect()
        exit()

    atexit.register(lambda: disconnect())

    if argv[1] == 'call':
        if (len(argv) - 1) < 3:
            print("invalid parameter")
            exit()

        object = argv[2]
        func = argv[3]
        if len(argv) >= 5:
            try:
                args = json.loads(argv[4])
            except Exception as e:
                print(e)
                exit()
        else:
            args = {}
        res = call(object, func, args)
        print(res)
    elif argv[1] == 'listen':
        if (len(argv) - 1) < 2:
            print("invalid parameter")
            exit()
        if not listen(argv[2], lambda v: print(v)):
            print(f'listen {argv[2]} failed!')
            exit()
        while True:
            time.sleep(3)
    elif argv[1] == 'send':
        if (len(argv) - 1) < 2:
            print("invalid parameter")
            exit()

        event = argv[2]
        if len(argv) >= 4:
            try:
                args = json.loads(argv[3])
            except Exception as e:
                print(e)
                exit()
        else:
            args = {}
        send(event, args)
    else:
        usage()
