#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner command client
"""
# fmt: off
__title__         = "ps_cmd"
__version__       = "0.1.0"
__date__          = "15 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import logging
import optparse
import sys
import time

from helpers.constants import *
import helpers.misc as misc
import libs.hydra as Hydra


LOG = logging.getLogger()
USAGE = "usage: %prog --addr [server_address] --cmd [command] <ARGS>"
EPILOG = """
Quickstart
====================
Run the script with a --addr and --cmd option. The --port is optional and has a default.
The --addr also has a default of the local host. If the script is being run on the same
machine as the server then you can omit the --addr parameter.

python ps_cmd.py --addr 1.2.3.4 --cmd dumpstate
python ps_cmd.py --addr 1.2.3.4 --cmd quit
python ps_cmd.py --cmd toggledebug

Return values
====================
0   No errors
"""


def parse_cli(argv, prog_ver, prog_date):
    # Create our command line parser. We use the older optparse library for compatibility on OneFS
    optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = optparse.OptionParser(
        usage=USAGE,
        version="%prog v" + prog_ver + " (" + prog_date + ")",
        epilog=EPILOG,
    )
    parser.add_option(
        "--addr",
        action="store",
        default=DEFAULT_LOOPBACK_ADDR,
        help="Server address (IP or FQDN)",
    )
    parser.add_option(
        "--port",
        action="store",
        type="int",
        default=DEFAULT_SERVER_PORT,
        help="Port number for client/server connection",
    )
    parser.add_option(
        "--cmd",
        type="choice",
        choices=(PS_CMD_DUMPSTATE, PS_CMD_QUIT, PS_CMD_TOGGLEDEBUG),
        default=None,
        help="""Command to send to the server.                                
dumpstate: Have the server and clients dump state to their    
-   respective loggers.                                       
quit: Send a graceful termination signal.                     
toggledebug: Toggles debug on and off. Same as sending a      
-   SIGUSR1 to the server process.                            
""",
    ),
    (raw_options, args) = parser.parse_args(argv[1:])
    return (parser, raw_options.__dict__, args)


class PSScanCommandClient(object):
    def __init__(self, args={}):
        self.commands = args.get("commands", [])
        self.server_addr = args.get("server_addr", DEFAULT_LOOPBACK_ADDR)
        self.server_port = args.get("server_port", DEFAULT_SERVER_PORT)
        self.socket = Hydra.HydraSocket(
            {
                "server_addr": self.server_addr,
                "server_port": self.server_port,
            }
        )

    def send_command(self):
        if not self.commands:
            LOG.info("No commands to send. No connection to server required.")
            return
        LOG.info("Connecting to server at {svr}:{port}".format(svr=self.server_addr, port=self.server_port))
        connected = self.socket.connect()
        if not connected:
            LOG.info("Unable to connect to server")
            return
        cmd = self.commands[0]
        LOG.info('Sending "{cmd}" command to server'.format(cmd=cmd))
        if cmd in (PS_CMD_DUMPSTATE, PS_CMD_TOGGLEDEBUG, PS_CMD_QUIT):
            self.socket.send({"type": MSG_TYPE_COMMAND, "cmd": cmd})
        else:
            LOG.error("Unknown command: {cmd}".format(cmd=cmd))
        time.sleep(1)
        self.socket.disconnect()


def main():
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)

    LOG.info("Sending command to server")
    client = PSScanCommandClient(
        {
            "args": args,
            "commands": options["cmd"],
            "options": options,
            "server_port": options["port"],
            "server_addr": options["addr"],
        }
    )
    try:
        client.send_command()
    except Exception as e:
        LOG.exception("Unhandled exception while sending command to server.")


if __name__ == "__main__" or __file__ == None:
    DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)
    # Disable loggers for sub modules
    for mod_name in ["libs.hydra"]:
        module_logger = logging.getLogger(mod_name)
        module_logger.setLevel(logging.WARN)
    main()
