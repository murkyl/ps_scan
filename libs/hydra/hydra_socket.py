#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__license__    = "MIT"
__author__     = "Andrew Chung"
__maintainer__ = "Andrew Chung"
__email__      = "Andrew.Chung@dell.com"
__credits__    = []
__copyright__  = """"""
__all__        = [
    "HydraSocket",
]
# fmt: on

import errno
import logging
import multiprocessing as mp
import pickle
import select
import socket
import struct
import threading
import time

from .hydra_const import *

try:
    dir(ConnectionResetError)
except:
    errno.EXFULL = 54

    class ConnectionResetError(Exception):
        pass


LOG = logging.getLogger(__name__)


class HydraSocket(object):
    """
    HydraSocket is a thin wrapper around a standard socket.

    There are 2 methods to initiate a connection.

    The first is "accept". This method is used to take an existing socket and wrap it with the methods provided by
    HydraSocket.

    The second method is "connect" which will create and initiate a socket connection to a remote server.

    To cleanup a HydraSocket, call the method "disconnect".

    To send data using this class, call the "send" method with the data to be sent. Sends are done asynchronously

    To receive data, call the "recv" method. If there is no data to be read, the method will return None. Received data
    will be returned as a Python dictionary object. The object will have the following format:
    {
      "type": ["data"|"cmd"],
      "data": <raw data>,
      "cmd": <cmd data>,
    }

    HydraSocket can be used in a select statement to monitor when data is available for reading.

    When the "type" is set to "data", the "data" field will be populated by data sent by the remote

    When the "type" is set to "cmd", this is a message from the class that informs the user of the class that the state
    of the socket has changed. The "cmd" field will contain the command data and the following strings currently
    supported:
      "closed": The HydraSocketHandler class is shut down

    If the command needs to provide additional data, that will be present in the "data" field.
    """

    def __init__(self, args=None):
        if not args:
            args = {}
        self.pipe_local, self.pipe_remote = mp.Pipe(duplex=True)
        self.keep_alive_interval = args.get("keep_alive_interval", DEFAULT_KEEP_ALIVE_INTERVAL)
        self.last_keepalive = time.time()
        self.last_keepalive_sent = 0
        self.poll_interval = args.get("poll_interval", DEFAULT_POLL_INTERVAL)
        self.remote_socket = None
        self.server_addr = args.get("server_addr", DEFAULT_BIND_ADDR)
        self.server_port = args.get("server_port", DEFAULT_SERVER_PORT)
        self.source_addr = args.get("source_addr", "")
        self.source_port = args.get("source_port", 0)
        self.thread_handler = None
        self.thread_shutdown = False
        self.wait_list = [self.pipe_local]
        self.wait_flush = mp.Event()

    def _get_socket_fd(self):
        try:
            return self.remote_socket.fileno()
        except Exception as e:
            if e.errno not in (errno.EBADF,):  # 9: Bad file descriptor
                LOG.error("Unknown exception when checking for socket fileno: {err}".format(err=e))
            # Python 2.7 doesn't support fileno() on closed sockets. Return -1 like Python 3
            return -1

    def _send_keepalive(self):
        if (time.time() - self.last_keepalive_sent) > self.keep_alive_interval:
            LOG.debug("Sending keepalive to remote")
            self._socket_send_header(HYDRA_MSG_TYPE["keepalive"])
            self.last_keepalive_sent = time.time()

    def _socket_recv(self, socket, length, chunk=DEFAULT_CHUNK_SIZE, raw=False):
        if not socket or self._get_socket_fd() == -1:
            raise EOFError("_socket_recv trying to read from closed socket: {sock}".format(sock=socket))
        data = bytearray(length)
        view = memoryview(data)
        while length:
            read_len = length if chunk > length else chunk
            bytes_recv = socket.recv_into(view, read_len)
            if bytes_recv == 0:
                raise EOFError("_socket_recv trying to read from closed socket: {sock}".format(sock=socket))
            view = view[bytes_recv:]
            length -= bytes_recv
        if not raw:
            data = pickle.loads(data)
        return data

    def _socket_recv_header(self, socket):
        """
        Decodes the Hydra header
        Current header consists of 1 longs (4 bytes), 2 shorts (2 bytes each) and 1 long (4 bytes)
        [  Magic Number  ][  Header Ver  ][  Message Type  ][ Message Length ]
        The only currently supported header version is 0x1
        """
        header = {"type": "invalid_msg", "ver": 0, "msg_length": 0}
        header_bytes = self._socket_recv(socket, HYDRA_HEADER_LEN, raw=True)
        if not header_bytes:
            raise EOFError("_socket_recv socket closed during read")
        # At this point, we are guaranteed that the header has enough bytes so we can
        # always decode the structure
        header_tuple = struct.unpack(HYDRA_HEADER_FORMAT, header_bytes)
        header["msg_length"] = header_tuple[3]
        header["ver"] = header_tuple[1]
        if header_tuple[0] == HYDRA_MAGIC and header_tuple[1] <= HYDRA_PROTOCOL_VER:
            # This is a valid message as the magic and protocol version matches
            # Set the header type to an appropriate value
            header["type"] = HYDRA_MSG_TYPE.get(header_tuple[2], "invalid_msg")
        return header

    def _socket_send(self, data):
        """Sends a Python object to the other end of the socket.
        The Python object must be able to be pickled.
        The method automatically adds in the required Hydra protocol header
        """
        bytes_data = pickle.dumps(data, pickle.HIGHEST_PROTOCOL)
        data_len = len(bytes_data)
        header_len = self._socket_send_header(HYDRA_MSG_TYPE["data"], data_len)
        try:
            self.remote_socket.sendall(bytes_data)
        except IOError as ioe:
            if ioe.errno in (
                errno.EBADF,  # 9: Bad file descriptor
                errno.EPIPE,  # 32: Bad pipe
            ):
                LOG.warning("Cannot send data. Socket not connected.")
                return 0
            raise
        return header_len + data_len

    def _socket_send_header(self, header_type, data_len=0):
        header = struct.pack(HYDRA_HEADER_FORMAT, HYDRA_MAGIC, HYDRA_PROTOCOL_VER, header_type, data_len)
        try:
            self.remote_socket.sendall(header)
        except IOError as ioe:
            if ioe.errno in (
                errno.EBADF,  # 9: Bad file descriptor
                errno.EPIPE,  # 32: Bad pipe
            ):
                LOG.warning("Cannot send header. Socket not connected.")
                return 0
            raise
        return HYDRA_HEADER_LEN

    def _start(self):
        if self.thread_handler:
            return False
        self.wait_list.append(self.remote_socket)
        self.last_keepalive_sent = time.time()
        self.thread_handler = threading.Thread(target=self._socket_processor, kwargs={})
        self.thread_handler.daemon = True
        self.thread_handler.start()
        return True

    def _socket_processor(self):
        try:
            while not self.thread_shutdown:
                # Send a keepalive message to the remote side if necessary
                self._send_keepalive()
                rlist, _, xlist = select.select(self.wait_list, [], self.wait_list, self.poll_interval)
                for s in rlist:
                    if s is self.pipe_local:
                        # We have data to send
                        while True:
                            data_avail = self.pipe_local.poll()
                            if not data_avail:
                                break
                            msg = self.pipe_local.recv()
                            sent_bytes = self._socket_send(msg)
                            if sent_bytes == 0:
                                LOG.debug("Socket send failed to send. Disconnecting and shutting down.")
                                raise EOFError("Socket closed during sending of data")
                        continue
                    # There are only 2 items in the wait list so if it is not the pipe_local it must be our socket
                    header = self._socket_recv_header(s)
                    if header["type"] == "data":
                        msg = self._socket_recv(s, header["msg_length"])
                        # LOG.debug("Received client data: {msg}".format(msg=msg))
                        self.pipe_local.send({"type": "data", "data": msg})
                        continue
                    if header["type"] == "keepalive":
                        LOG.debug("Received remote keepalive")
                        self.last_keepalive = time.time()
                        continue
                    if header["type"] == "flush":
                        LOG.debug("Received {type}".format(type=header["type"]))
                        # Echo the flush back to the sender
                        self._socket_send_header(HYDRA_MSG_TYPE["flush_ack"])
                        continue
                    if header["type"] == "flush_ack":
                        LOG.debug("Received {type}".format(type=header["type"]))
                        self.wait_flush.set()
                        continue
                    LOG.warning("Invalid header. Removing client from active list. Header: {data}".format(data=header))
                    self.disconnect()
                if xlist:
                    LOG.debug("Select returned an exception list")
                    break
        except IOError as ioe:
            if ioe.errno in (errno.EBADF,):
                # 9: Bad file descriptor
                LOG.debug("Socket closed while waiting in select")
            elif ioe.errno in (
                errno.EPIPE,  # 32: Broken pipe
                errno.EXFULL,  # 54: Exchange full
                errno.ECONNRESET,  # 104: Connection reset by peer
                errno.ENOTCONN,  # 107: Transport not connected
            ):
                LOG.debug("Connection closed for: {sock} - {err}".format(err=ioe, sock=self.remote_socket))
            else:
                LOG.info("Unexpected IOError reading socket: {sock} - {err}".format(err=ioe, sock=self.remote_socket))
        except EOFError as eofe:
            LOG.debug("Receive connection closed for: {sock} - {err}".format(err=eofe, sock=self.remote_socket))
        except ConnectionResetError as cre:
            LOG.debug("Socket connection reset for: {sock} - {err}".format(err=cre, sock=self.remote_socket))
        except Exception as e:
            if e.errno == errno.ECONNRESET:
                # 104: Connection reset by peer
                LOG.debug("Socket connection reset for: {sock} - {err}".format(err=cre, sock=self.remote_socket))
            else:
                LOG.exception("Unexpected exception occurred in _socket_processor: {err}".format(err=e))
        LOG.debug("_socket_processor exiting")
        self.pipe_local.send(CMD_MSG_SOCKET_CLOSED)
        self.disconnect()
        self.thread_handler = None
        self.pipe_local.close()

    def accept(self, remote_socket):
        if self.remote_socket:
            LOG.warning("HydraSocket already in use")
            return False
        self.remote_socket = remote_socket
        self.remote_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
        self.source_addr = self.remote_socket.getsockname()[0]
        self.source_port = self.remote_socket.getsockname()[1]
        LOG.debug("Accepted socket: {data}".format(data=self.remote_socket))
        self._start()
        return True

    def connect(self, server_addr=None, server_port=None, source_addr=None, source_port=None):
        if self.remote_socket:
            LOG.warning("HydraSocket already in use")
            return False
        if server_addr:
            self.server_addr = server_addr
        if server_port:
            self.server_port = server_port
        if source_addr:
            self.source_addr = source_addr
        if source_port:
            self.source_port = source_port
        try:
            LOG.debug("Connecting to: {addr}:{port}".format(addr=self.server_addr, port=self.server_port))
            self.remote_socket = socket.create_connection(
                (self.server_addr, self.server_port),
                socket.getdefaulttimeout(),
                (self.source_addr, self.source_port),
            )
            self.remote_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, True)
            self.source_addr = self.remote_socket.getsockname()[0]
            self.source_port = self.remote_socket.getsockname()[1]
        except Exception as e:
            LOG.info(
                "Unable to connect to server %s:%s. Check address, port and firewalls."
                % (self.server_addr, self.server_port)
            )
            self.remote_socket = None
            return False

        LOG.debug(
            "Connected to remote host on local socket: {addr}:{port}".format(
                addr=self.source_addr, port=self.source_port
            )
        )
        self._start()
        return True

    def disconnect(self):
        if not self.remote_socket:
            return False
        if self._get_socket_fd() == -1:
            return True
        self.thread_shutdown = True
        self.wait_list[:] = []
        try:
            self.remote_socket.shutdown(socket.SHUT_RDWR)
        except:
            pass
        try:
            self.remote_socket.close()
        except:
            pass
        return True

    def fileno(self):
        return self.pipe_remote.fileno()

    def flush(self):
        if not self.remote_socket:
            LOG.warning("Cannot flush socket. Socket not connected.")
            return None
        if self._get_socket_fd() == -1:
            return True
        self.wait_flush.clear()
        self._socket_send_header(HYDRA_MSG_TYPE["flush"])
        self.wait_flush.wait(DEFAULT_SHUTDOWN_WAIT_TIMEOUT)

    def send(self, msg):
        """
        Push 'msg' onto an output queue for asynchronous serialization and transmit
        """
        if not self.remote_socket:
            LOG.warning("Cannot send message. Socket not connected.")
            return False
        try:
            self.pipe_remote.send(msg)
        except IOError as ioe:
            if ioe.errno in (errno.EPIPE,):  # 32: Bad pipe
                LOG.info("Cannot sent message. Pipe to send messages has been closed.")
        return True

    def recv(self):
        if not self.remote_socket:
            LOG.warning("Cannot receive message. Socket not connected.")
            return None
        data_avail = self.pipe_remote.poll()
        if not data_avail:
            return None
        try:
            msg = self.pipe_remote.recv()
        except Exception as e:
            return None
        return msg
