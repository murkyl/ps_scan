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
import platform
import sys

import elasticsearch_wrapper
import helpers.cli_parser as cli_parser
from helpers.constants import *
import helpers.misc as misc
import libs.hydra as Hydra
import ps_scan_client as psc
import ps_scan_server as pss
import user_handlers


DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
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

    setup_logger(options)

    es_options = {}
    if options["es_options_file"]:
        es_options = misc.read_es_options_file(options["es_options_file"])
        if es_options is None:
            LOG.critical("Unable to open or read the credentials file: {file}".format(file=options["es_cred_file"]))
            sys.exit(3)
        # Take CLI options over file ones if they are present
        # Since we read in some options from a file, fill in the options dictionary with the file values if needed
        for key in ["es_index", "es_pass", "es_type", "es_url", "es_user"]:
            if options[key]:
                es_options[key[3:]] = options[key]
            elif es_options.get(key):
                options[key] = es_options[key[3:]]
        # Fix default for es_type parameter if neither the file nor CLI has this parameter
        if not es_options.get("es_type"):
            es_options["type"] = ES_TYPE_PS_SCAN
            options["es_type"] = ES_TYPE_PS_SCAN
    elif (
        options["es_index"] is not None
        and options["es_user"] is not None
        and options["es_pass"] is not None
        and options["es_url"] is not None
    ):
        es_options = {
            "index": options["es_index"],
            "password": options["es_pass"],
            "url": options["es_url"],
            "user": options["es_user"],
            "type": options.get("es_type", ES_TYPE_PS_SCAN),
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
        LOG.info("Starting client")
        options["scanner_file_handler"] = user_handlers.file_handler_pscale
        options["server_addr"] = options["addr"]
        options["server_port"] = options["port"]
        client = psc.PSScanClient(options)
        try:
            client.connect()
        except Exception as e:
            LOG.exception("Unhandled exception in client.")
    elif options["op"] in (OPERATION_TYPE_AUTO, OPERATION_TYPE_SERVER):
        ps_scan_server_options = {
            "cli_options": options,
            "client_config": {},
            "node_list": None,
            "scan_path": args,
            "script_path": os.path.abspath(__file__),
            "server_addr": options["addr"],
            "server_connect_addr": misc.get_local_internal_addr() or DEFAULT_LOOPBACK_ADDR,
            "server_port": options["port"],
            "stats_handler": user_handlers.print_statistics,
        }
        if options["op"] == "auto":
            if options["type"] == SCAN_TYPE_ONEFS:
                local_lnn = misc.get_local_node_number()
                # If there is no node list from the CLI and we are set to auto and running on OneFS, automatically set
                # the node list to the local machine and run 1 client. Otherwise parse the nodes parameter and return
                # a node list to run clients on.
                node_list = misc.parse_node_list(options["nodes"], min_node_list=[local_lnn])
                # Setting the node list will cause the server to automatically launch clients
                ps_scan_server_options["node_list"] = node_list
        try:
            es_client = None
            es_cmd_idx = {}
            if es_options:
                es_client, es_cmd_idx = elasticsearch_wrapper.es_create_connection(
                    es_options["url"],
                    es_options["user"],
                    es_options["password"],
                    es_options["index"],
                    es_options["type"],
                )
                ps_scan_server_options["client_config"]["es_options"] = es_options
                ps_scan_server_options["client_config"]["es_cmd_idx"] = es_cmd_idx
                ps_scan_server_options["client_config"]["es_send_threads"] = options["es_threads"]
            if es_client and (options["es_init_index"] or options["es_reset_index"]):
                if options["es_reset_index"]:
                    elasticsearch_wrapper.es_delete_index(es_client, es_cmd_idx)
                LOG.debug("Initializing indices for Elasticsearch: {index}".format(index=es_options["index"]))
                es_index_settings = elasticsearch_wrapper.es_create_index_settings(
                    {
                        "number_of_shards": options["es_shards"],
                        "number_of_replicas": options["es_replicas"],
                        "type": es_options["type"],
                    }
                )
                elasticsearch_wrapper.es_init_index(es_client, es_cmd_idx, es_index_settings, options)
            if es_client:
                start_options = elasticsearch_wrapper.es_create_start_options(options)
                elasticsearch_wrapper.es_start_processing(es_client, es_cmd_idx, start_options, options)
            svr = pss.PSScanServer(ps_scan_server_options)
            svr.serve()
            if es_client:
                stop_options = elasticsearch_wrapper.es_create_stop_options(options)
                elasticsearch_wrapper.es_stop_processing(es_client, es_cmd_idx, stop_options, options)
        except Exception as e:
            LOG.exception("Unhandled exception in server.")


def setup_logger(options):
    if options.get("log"):
        base, ext = os.path.splitext(options.get("log", DEFAULT_LOG_FILE_PREFIX + DEFAULT_LOG_FILE_SUFFIX))
        format_string_vars = {
            "hostname": platform.node(),
            "pid": os.getpid(),
            "prefix": base,
            "suffix": ext,
        }
        log_handler = logging.FileHandler(DEFAULT_LOG_FILE_FORMAT.format(**format_string_vars))
    else:
        log_handler = logging.StreamHandler()
    debug_count = options.get("debug", 0)
    log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
    LOG.addHandler(log_handler)
    if debug_count:
        LOG.setLevel(logging.DEBUG)
    else:
        LOG.setLevel(logging.INFO)
    if debug_count < 3:
        # Disable loggers for hydra sub modules
        for mod_name in ["libs.hydra", "helpers.papi_lite"]:
            module_logger = logging.getLogger(mod_name)
            module_logger.setLevel(logging.WARN)


if __name__ == "__main__" or __file__ == None:
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
