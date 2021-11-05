# cython: language_level=3
import socket
import sys

import Stockings
import logging
import logging.handlers
from threading import Thread, Lock, active_count
from time import sleep
import json
import uuid
import platform
from common import *
from concurrent.futures import ThreadPoolExecutor

thread_pool = ThreadPoolExecutor(max_workers=10)  # pool for parse incoming data

log = logging.getLogger("client")
log.setLevel(logging.DEBUG)

# create the logging file handler
fh = logging.handlers.RotatingFileHandler("client.log", mode='a', maxBytes=512*1024*1024, backupCount=7)  # 512mb**8 = 4096mb

formatter = logging.Formatter('%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s')  # - %(name)s
formatter.converter = time.gmtime
fh.setFormatter(formatter)

# add handler to logger object
log.addHandler(fh)


def handle_exception(exc_type, exc_value, exc_traceback):
    log.error("Important! Uncaught exception:", exc_info=(exc_type, exc_value, exc_traceback))
    sys.__excepthook__(exc_type, exc_value, exc_traceback)


sys.excepthook = handle_exception


sh = logging.StreamHandler()
sh.setLevel(logging.INFO)
format_short = logging.Formatter('%(asctime)s - %(message)s')
sh.setFormatter(format_short)
log.addHandler(sh)

SOCKET_IP = 'localhost'
SOCKET_PORT = 5005


class AuthClientStocking(Stockings.Stocking):

    def __init__(self, sock, user, password, pc):
        self.user = user
        self.password = password
        self.pc = pc

        super().__init__(sock)

    def handshake(self):
        """ Sends our unique ID to the remote and receives the remotes unique ID. """

        user = self.user + "~priority" + f"~{PRIORITY_TIME}" if PRIORITY_CONNECTION else self.user
        auth_string = json.dumps((user, self.password, self.pc, 'default', VERSION)) #, platform.architecture()[0]))
        # TODO architecture call uncompatible with Gevent!

        log.info(f"authing client with {auth_string}")

        self._write(str(auth_string))

        try:
            while self.active:
                read = self._read()
                if read is not None:
                    remote_auth = read
                    log.info(f"remote auth: {remote_auth}")
                    if remote_auth == "server_ok":
                        log.info("authorized")
                        return True
                    else:
                        log.info("not authorized!")
                        return False
        except EOFError:
            log.error("EOFerror!")
        except Exception as e:
            pass

        print(f"bad handshake")
        return False


class ClientWrapper:

    def __init__(self, pc):
        self.socket = None
        self.conn = None
        self.pc = pc
        self.user = None

        self.connecting = True

        self.acks = dict()
        self.acks_lock = Lock()

        self.ping_thread = Thread(target=self._ping_thread)
        self.ping_thread.start()

        self.ack_thread = Thread(target=self._ack_thread)
        self.ack_thread.start()

    def _ping_thread(self):
        while True:
            log.info(f"number of threads: {active_count()}")
            sleep(10)
            if self.conn and self.conn.active:
                try:
                    self.ping()
                except Exception as e:
                    log.warning(f"can't ping, {e}")

    def _ack_thread(self):
        while True:
            sleep(0.5)
            if self.connecting:
                continue
            now = datetime.now().timestamp()
            with self.acks_lock:
                # log.info(f"acks.items() = {self.acks.items()}")
                for aid, (dt, type_, data) in self.acks.items():
                    # log.info(f"diff = {now - dt}, timeout = {ACK_TIMEOUT}")
                    if now - dt > ACK_TIMEOUT:
                        log.error("ack: connection may be broken! closing connection...")
                        log.error(f"ack: aid: {aid}, dt: {dt}, type: {type_}, data: {data}")
                        log.error(f"ack: self.connecting: {self.connecting}")
                        try:
                            log.info(f"ack: stocking queued data: {self.conn.writeDataQueued()}")
                            self.close()
                        except Exception as e:
                            log.error(f"ack: got exception while close (handle is closed? connecting is True?): {e}")
                        self.connecting = True
                        # TODO: resend not sent

    def connect(self, user, password, pc):
        self.user = user
        if self.socket:
            self.socket.close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            log.info(f"connecting to server...")
            self.socket.connect((SOCKET_IP, SOCKET_PORT))
        except ConnectionRefusedError as e:
            log.error(f"Connection refused! {e}")
            return False
        except Exception as e:
            log.error(f"Server is not available, unknown error: {e}")
            return False

        self.conn = AuthClientStocking(self.socket, user=user, password=password, pc=pc)
        tries = 0
        while not self.conn.handshakeComplete and self.conn.active and tries < 20:
            log.info("client: waiting for handshake")  # TODO: spins here forever sometines
            sleep(1)
            tries += 1
        if self.conn.handshakeComplete:
            log.info(f"client: handshake completed, tries: {tries}")
            self.connecting = False
            with self.acks_lock:
                self.acks = dict()
        else:
            log.info(f"client: handshake failed, tries: {tries}")
            return

        self._read_handler()

    def _read_handler(self):
        fd = self.socket.fileno()
        try:
            while self.conn.active: #and not self.need_close:
                data = self.conn.read()
                if data is not None:
                    log.debug(f"read from {fd}: {data}")
                    self._parse(data)
                sleep(0.1)  # 0.01 todo: rewrite blocking
            log.warning("connection closed!")
        except EOFError as e:
            log.error(f"{fd} handler got EOF {e}")
        except Exception as e:
            log.error(f"{fd} handler got unknown exception: {e}")
            log.error(traceback.format_exc())

    def _uid(self):
        return uuid.uuid4().hex[:6]

    def _parse(self, data):
        splitter = data.index(':')
        type_, json_ = data[:splitter], data[splitter + 1:]
        data = self._deserialize(json_)
        if type_ != "ack":
            self._ack(type_, data)
        else:
            self._handle_ack(data['id'])
        # TODO: is blocking, or thread without locks, make scan-lock
        thread_pool.submit(self._dispatch, type_, data)

    def _dispatch(self, t, data):
        log.debug("dispatching base")

    # blocking todo
    def _write(self, st):
        try:
            self.conn.write(st)
            while self.conn.writeDataQueued():
                sleep(.01)
        except Exception as e:
            log.error(f"_write: can't perform write, exc: {e}, {traceback.format_exc()}")
        # TODO: wait acc

    def _respond(self, type_, data):
        if self.conn and self.conn.active:
            if 'id' not in data:
                data['id'] = self._uid()

            if type_ != 'ack':
                with self.acks_lock:
                    self.acks[data['id']] = (datetime.now().timestamp(), type_, data)

            data['from'] = self.pc
            j = self._serialize(data)
            self._write(f"{type_}:{j}")
            if type_ != 'ack' and type_ != 'ping':
                log.info(f"sent {type_}-{data['type']} with id={data['id']} to server")

        log.info(f"_respond client function with id {data['id']} is done!")

    def ask(self, data):
        data['initiator'] = self.pc
        return self._respond("ask", data)

    def ans(self, recv_data, data):
        data['id'] = recv_data['id']
        data['initiator'] = recv_data['initiator']
        return self._respond("ans", data)

    def _handle_ack(self, id_):
        with self.acks_lock:
            log.debug("S_CLIENT IS UPDATED...")
            log.debug(f"got ack for {id_}")
            if id_ in self.acks:
                self.acks.pop(id_)

    def _ack(self, type_, recv_data):
        log.debug(f"acking {recv_data['id']}")
        data = dict()
        data['id'] = recv_data['id']
        data['req_type'] = type_
        data['type'] = recv_data['type']
        data['who'] = 'client'  # todo: s-client or c-client
        data['initiator'] = recv_data['initiator']
        self._respond("ack", data)

    def ping(self):
        self._respond('ping', {})

    def close(self):
        try:
            # shutdown gracefully (windows likes it)
            if platform.system() == 'Windows':
                if self.socket:
                    self.socket.shutdown(socket.SHUT_WR)
                sleep(1)

            if self.conn:
                log.info("closing stocking...")
                # with self.io_lock:
                self.conn.close()
                log.info("closed stocking")
            else:
                log.warning("stocking conn is non existent!")

        except Exception as e:
            log.error(f"can't close socket connection, got exc: {e}, {traceback.format_exc()}")

    def _deserialize(self, msg):
        return json.loads(msg)

    def _serialize(self, data):
        return json.dumps(data)

    def _acc(self, msg):
        pass
