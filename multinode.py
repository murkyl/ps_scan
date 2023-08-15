#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "0.1.0"
__date__          = "12 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import json
import logging
import multiprocessing as mp
import os
import sys

import elasticsearch_wrapper
import helpers.cli_parser as cli_parser
from helpers.constants import *
import helpers.misc as misc
import libs.hydra as Hydra
import ps_scan_client as psc
import ps_scan_server as pss
import user_handlers


LOG = logging.getLogger()


def main():
    # Setup command line parser and parse agruments
    (parser, options, args) = cli_parser.parse_cli(sys.argv, __version__, __date__)

    # Validate command line options
    cmd_line_errors = []
    if len(args) == 0 and options["op"] in (OPERATION_TYPE_AUTO, OPERATION_TYPE_SERVER):
        cmd_line_errors.append("***** A minimum of 1 path to scan is required to be specified on the command line.")
    if cmd_line_errors:
        parser.print_help()
        sys.stderr.write("\n" + "\n".join(cmd_line_errors) + "\n")
        sys.exit(1)

    es_credentials = {}
    if options["es_cred_file"]:
        es_credentials = misc.read_es_cred_file(options["es_cred_file"])
        if es_credentials is None:
            LOG.critical("Unable to open or read the credentials file: {file}".format(file=filename))
            sys.exit(3)
    elif options["es_index"] and options["es_user"] and options["es_pass"] and options["es_url"]:
        es_credentials = {
            "index": options["es_index"],
            "password": options["es_pass"],
            "url": options["es_url"],
            "user": options["es_user"],
        }

    if options["type"] == SCAN_TYPE_AUTO:
        if misc.is_onefs_os():
            options["type"] = SCAN_TYPE_ONEFS
        else:
            options["type"] = SCAN_TYPE_BASIC
    if options["type"] == SCAN_TYPE_ONEFS:
        if not misc.is_onefs_os():
            sys.stderr.write(
                "Script is not running on a OneFS operation system. Invalid --type option, use 'basic' instead.\n"
            )
            sys.exit(2)
        # Set resource limits
        old_limit, new_limit = misc.set_resource_limits(options["ulimit_memory"])
        if new_limit:
            LOG.debug("VMEM ulimit value set to: {val}".format(val=new_limit))
        else:
            LOG.info("VMEM ulimit setting failed.")
        file_handler = user_handlers.file_handler_pscale
    else:
        file_handler = user_handlers.file_handler_basic
    LOG.debug("Parsed options:\n{opt}".format(opt=json.dumps(options, indent=2, sort_keys=True)))
    LOG.debug("Initial scan paths: {paths}".format(paths=", ".join(args)))

    if options["op"] == OPERATION_TYPE_CLIENT:
        # DEBUG: START
        import platform
        log_filename = "init-%s-%s.txt"%(platform.node(), os.getpid())
        log_handler = logging.FileHandler(log_filename)
        LOG.addHandler(log_handler)
        LOG.setLevel(logging.DEBUG)
        # DEBUG: END
        LOG.info("Starting client")
        client = psc.PSScanClient(
            {
                "server_port": options["port"],
                "server_addr": options["addr"],
                "scanner_file_handler": user_handlers.file_handler_pscale,
            }
        )
        try:
            client.connect()
        except Exception as e:
            LOG.exception("Unhandled exception in client.")
    elif options["op"] in (OPERATION_TYPE_AUTO, OPERATION_TYPE_SERVER):
        node_list = misc.parse_node_list(options["nodes"])
        ps_scan_server_options = {
            "cli_options": options,
            "client_config": {},
            "scan_path": args,
            "script_path": os.path.abspath(__file__),
            "server_port": options["port"],
            "server_addr": options["addr"],
            "server_connect_addr": misc.get_local_internal_addr() or DEFAULT_LOOPBACK_ADDR,
            "node_list": None,
        }
        if es_credentials:
            ps_scan_server_options["client_config"]["es_credentials"] = es_credentials
            ps_scan_server_options["client_config"]["es_send_threads"] = options["es_threads"]
        if options["op"] == "auto" and node_list:
            # Setting the node list will cause the server to automatically launch clients
            ps_scan_server_options["node_list"] = node_list

        try:
            es_client = None
            if es_credentials:
                es_client = elasticsearch_wrapper.es_create_connection(
                    es_credentials["url"],
                    es_credentials["user"],
                    es_credentials["password"],
                    es_credentials["index"],
                )
            if es_client and (options["es_init_index"] or options["es_reset_index"]):
                if options["es_reset_index"]:
                    elasticsearch_wrapper.es_delete_index(es_client)
                LOG.debug("Initializing indices for Elasticsearch: {index}".format(index=es_credentials["index"]))
                es_index_settings = elasticsearch_wrapper.es_create_index_settings(
                    {
                        "number_of_shards": options["es_shards"],
                        "number_of_replicas": options["es_replicas"],
                    }
                )
                elasticsearch_wrapper.es_init_index(es_client, es_credentials["index"], es_index_settings)
            if es_client:
                elasticsearch_wrapper.es_start_processing(es_client, {})
            svr = pss.PSScanServer(ps_scan_server_options)
            svr.serve()
            if es_client:
                elasticsearch_wrapper.es_stop_processing(es_client, {})
        except Exception as e:
            LOG.exception("Unhandled exception in server.")


if __name__ == "__main__" or __file__ == None:
    DEFAULT_LOG_FORMAT = (
        "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - (%(process)d|%(threadName)s) %(message)s"
    )
    log_handler = logging.StreamHandler()
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    LOG.setLevel(logging.DEBUG)
    # Disable loggers for sub modules
    for mod_name in ["libs.hydra"]:
        module_logger = logging.getLogger(mod_name)
        module_logger.setLevel(logging.WARN)
    # Support scripts built into executable on Windows
    try:
        mp.freeze_support()
        if hasattr(mp, "set_start_method"):
            # Force all OS to behave the same when spawning new process
            mp.set_start_method("spawn")
    except:
        # Ignore these errors as they are either already set or do not apply to this system
        pass
    main()
