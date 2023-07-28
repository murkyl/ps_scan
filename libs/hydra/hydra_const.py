#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__license__    = "MIT"
__author__     = "Andrew Chung"
__maintainer__ = "Andrew Chung"
__email__      = "Andrew.Chung@dell.com"
__credits__    = []
__copyright__ = """"""
# fmt: on

import struct

CMD_MSG_SOCKET_CLOSED = {"type": "cmd", "cmd": "closed"}

DEFAULT_ASYNC_SERVER = False
DEFAULT_BIND_ADDR = "0.0.0.0"
DEFAULT_CHUNK_SIZE = 131072
DEFAULT_KEEP_ALIVE_INTERVAL = 10
DEFAULT_POLL_INTERVAL = 0.25
DEFAULT_SERVER_ADDR = "0.0.0.0"
DEFAULT_SERVER_PORT = 49372
DEFAULT_SHUTDOWN_WAIT_TIMEOUT = 10
DEFAULT_SOCKET_LISTEN_QUEUE = 10
DEFAULT_SSL_ENABLED = False
HYDRA_HEADER_FORMAT = "!LHHL"
HYDRA_HEADER_LEN = struct.calcsize(HYDRA_HEADER_FORMAT)
HYDRA_MAGIC = 0xC0DCC0DC
HYDRA_MSG_TYPE = {
    0x1: "keepalive",
    "keepalive": 0x1,
    0x2: "closed",
    "closed": 0x2,
    0x3: "flush",
    "flush": 0x3,
    0x4: "flush_ack",
    "flush_ack": 0x4,
    0xA: "data",
    "data": 0xA,
}
HYDRA_PROTOCOL_VER = 0x1
