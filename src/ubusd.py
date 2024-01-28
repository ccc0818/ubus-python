import json
import logging
import socket
import select
from threading import Thread, Lock
from concurrent.futures import ThreadPoolExecutor
import time
from pathlib import Path


def init_logging():
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(lineno)d - %(message)s')
    logging.getLogger().setLevel(logging.DEBUG)

def i2b(i: int) -> bytes:
    return i.to_bytes(4, 'little')

def b2i(b: bytes) -> int:
    return int.from_bytes(b, 'little')

class Client:
    def __init__(self, sock: socket.socket, address):
        self.sock = sock
        self.address = address
        self._lock = Lock()
        self._objects: dict[str, set] = {}
        self._objects_lock = Lock()
        self._listen: set[str] = set()
        self._listen_lock = Lock()

    def __del__(self):
        self.close()

    def close(self):
        with self._lock:
            if self.sock:
                self.sock.close()
                self.sock = None
                logging.critical(f'client {self.address} closed !')

    def _send(self, data: bytes):
        with self._lock:
            self.sock.send(data)

    def object_exist(self, object: str) -> bool:
        with self._objects_lock:
            return object in self._objects

    def add_object(self, name: str, funcs: set):
        with self._objects_lock:
            self._objects[name] = set(funcs)

    def send_add_reply(self, data: dict):
        data = json.dumps(data).encode()
        data_len = i2b(len(data) + 1)
        self._send(data_len + b'\xf0' + data)

    def call(self, cs: socket.socket, call_id: str, object: str, func: str, data: dict):
        with self._objects_lock:
            if object in self._objects and func in self._objects[object]:
                msg = {}
                msg['_cs'] = cs.fileno()
                msg['_id'] = call_id
                msg['object'] = object
                msg['func'] = func
                msg['data'] = data
                msg = json.dumps(msg).encode()
                data_len = i2b(len(msg) + 1)
                self._send(data_len + b'\xf2' + msg)
            else:
                self.send_call_reply({'_id': call_id, 'ret': 0})

    def send_call_reply(self, data: dict):
        data = json.dumps(data).encode()
        data_len = i2b(len(data) + 1)
        self._send(data_len + b'\xf1' + data)

    def listen(self, id: str, event: str):
        with self._listen_lock:
            self._listen.add(event)
        data = json.dumps({'_id': id, 'ret': 1}).encode()
        data_len = i2b(len(data) + 1)
        self._send(data_len + b'\xf3' + data)

    def notify(self, event: str, data: dict):
        with self._listen_lock:
            msg = {
                'event': event,
                'data': data
            }
            if event in self._listen:
                msg = json.dumps(msg).encode()
                data_len = i2b(len(msg) + 1)
                self._send(data_len + b'\xf4' + msg)

class Server(Thread):
    sock_path = Path("/var/tmp/ubus.sock")
    backlog = 32

    def __init__(self):
        super().__init__()
        self.daemon = True
        if self.sock_path.exists():
            self.sock_path.unlink()
        self._sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(self.sock_path.as_posix())
        self._sock.listen(self.backlog)
        self._rfds = {self._sock}
        self._clients: dict[socket.socket, Client] = {}
        self._clients_lock = Lock()
        self._thread_pool = ThreadPoolExecutor(5)

    def __del__(self):
        self._thread_pool.shutdown()
        for s in self._rfds:
            s.close()

    def _connect_handler(self):
        cs, address = self._sock.accept()
        self._rfds.add(cs)
        with self._clients_lock:
            self._clients[cs] = Client(cs, address)
        logging.critical(f"client {address} connected .")

    def _client_msg_handler(self, cs: socket.socket, msg: bytes):
        # length(4) + type(1) + value
        try:
            t = msg[0]
            data = json.loads(msg[1:])
            if t == 0x00:
                self._add_handler(cs, data)
            elif t == 0x01:
                self._call_handler(cs, data)
            elif t == 0x02:
                self._call_reply_handler(cs, data)
            elif t == 0x03:
                self._listen_handler(cs, data)
            elif t == 0x04:
                self._send_handler(cs, data)
            else:
                logging.error(f'type error {t}')
        except Exception as e:
            logging.error(e)

    def _add_handler(self, cs: socket.socket, data: dict):
        # data: {'_id': uuid, object': name, 'funcs': []}
        # reply: 0xf0 {'ret': 0/1}
        object_name = data['object']
        with self._clients_lock:
            for client in self._clients.values():
                if client.object_exist(object_name):
                    self._clients[cs].send_add_reply({'_id': data['_id'], 'ret': 0})
                    return

            self._clients[cs].add_object(object_name, data['funcs'])
            self._clients[cs].send_add_reply({'_id': data['_id'], 'ret': 1})

    def _call_handler(self, cs: socket.socket, data: dict):
        # data: {'object': name, func: f, _id: uuid, data: {}}
        # reply: 0xf1 {'ret': 0/1, data: {}}
        with self._clients_lock:
            for client in self._clients.values():
                if client.object_exist(data['object']):
                    client.call(cs, data['_id'], data['object'], data['func'], data['data'])
                    return
            self._clients[cs].send_call_reply({"_id": data['_id'], 'ret': 0})

    def _call_reply_handler(self, cs: socket.socket, data: dict):
        # data: {'_id': uuid, '_cs': cs, data: res}
        with self._clients_lock:
            client = None
            for c in self._clients.keys():
                if c.fileno() == data['_cs']:
                    client = self._clients[c]

            client and client.send_call_reply({'_id': data['_id'], 'ret': 1, 'data': data['data']})

    def _listen_handler(self, cs: socket.socket, data: dict):
        # data: {'_id': uuid, "event": str}
        with self._clients_lock:
            client = self._clients[cs]
            client.listen(data['_id'], data['event'])

    def _send_handler(self, cs: socket.socket, data: dict):
        # data: {"event": str, data: {}}
        with self._clients_lock:
            for client in self._clients.values():
                client.notify(data['event'], data['data'])

    def run(self):
        while True:
            try:
                r, _, _ = select.select(self._rfds, [], [])
                for s in r:
                    if s == self._sock:
                        self._connect_handler()
                    else:
                        msg = s.recv(b2i(s.recv(4, socket.MSG_WAITALL)), socket.MSG_WAITALL)
                        if len(msg):
                            self._thread_pool.submit(self._client_msg_handler, s, msg)
                        else:
                            with self._clients_lock:
                                del self._clients[s]
                            self._rfds.remove(s)
            except Exception as e:
                logging.error(e)

if __name__ == "__main__":
    init_logging()

    server = Server()
    server.start()
    logging.critical('ubusd start run .')

    while True:
        time.sleep(3)

        if not server.is_alive():
            break
