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
        logging.warn(f'request {type} timeout !')
        with _request_map_lock:
            del _request_map[id]
        return None

    res = _request_map[id]['data']
    with _request_map_lock:
        del _request_map[id]

    return res

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
                res = cb(data['data'])
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
                        cb(data['data'])
        except Exception as e:
            logging.error(e)

def connect() -> bool:
    global _sock, _sock_lock
    with _sock_lock:
        try:
            _sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            _sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            _sock.connect('/var/tmp/ubus.sock')
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
    msg = {
        'event': event,
        'data': data
    }
    _send(b'\x04', msg)

def listen(event: str, cb: Callable[[dict], any]) -> bool:
    global _event_map_lock, _event_map
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
    _disconnect_cb = cb
