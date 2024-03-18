#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "0.3.2"
__date__          = "24 January 2024"
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

import helpers.cli_parser as cli_parser
from helpers.constants import *
import helpers.elasticsearch_wrapper as elasticsearch_wrapper
import helpers.misc as misc
import helpers.ps_scan_client as psc
import helpers.ps_scan_server as pss
import helpers.user_handlers as user_handlers
import helpers.scanner as scanner
import libs.hydra as Hydra
from libs.onefs_become_user import become_user


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

    es_options = {}
    if options["es_options_file"]:
        try:
            es_options = misc.read_es_options_file(options["es_options_file"])
        except Exception as e:
            sys.stderr.write(str(e) + "\n")
            sys.exit(3)
        # Take CLI options over file ones if they are present
        # Since we read in some options from a file, fill in the options dictionary with the file values if needed
        for key in ["es_index", "es_pass", "es_type", "es_url", "es_user"]:
            if options[key]:
                es_options[key[3:]] = options[key]
            elif es_options.get(key[3:]):
                options[key] = es_options[key[3:]]
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

    if options["csv_output_path"]:
        if not os.path.isdir(options["csv_output_path"]):
            sys.stderr.write("CSV output path option is not a directory\n")
            sys.exit(2)
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
            if options["debug"] > 0:
                sys.stderr.write("VMEM ulimit value set: %s\n" % new_limit)
        else:
            if options["debug"] > 0:
                sys.stderr.write("VMEM ulimit setting failed\n")
        file_handler = scanner.file_handler_pscale
    else:
        file_handler = scanner.file_handler_basic

    if options["op"] == OPERATION_TYPE_CLIENT:
        options["nodepool_translation"] = misc.get_nodepool_translation()
        options["scanner_file_handler"] = file_handler
        options["server_addr"] = options["addr"]
        options["server_port"] = options["port"]
        # If the --user CLI parameter is passed in, try and change to that user. If that fails, exit immediately.
        if options.get("user"):
            try:
                become_user(options["user"])
            except Exception as e:
                sys.stderr.write("Unable to setuid to user: %s" % options["user"])
                sys.exit(5)
        setup_logger(options)
        LOG.info({"msg": "Starting client"})
        LOG.debug(
            {
                "msg": "Parsed options",
                "options": json.dumps(options, indent=2, sort_keys=True, default=lambda o: "<not serializable>"),
            }
        )
        client = psc.PSScanClient(options)
        try:
            client.connect()
        except Exception as e:
            LOG.exception({"msg": "Unhandled exception in client", "exception": str(e)})
    elif options["op"] in (OPERATION_TYPE_AUTO, OPERATION_TYPE_SERVER):
        options["scan_path"] = args if not isinstance(args, list) else args[0]
        connect_addr = options["addr"]
        if connect_addr in [DEFAULT_SERVER_ADDR, "::", "[::1]"]:
            connect_addr = misc.get_local_internal_addr() or DEFAULT_LOOPBACK_ADDR
        ps_scan_server_options = {
            "cli_options": options,
            "client_config": {},
            "node_list": None,
            "scan_path": args,
            "script_path": os.path.abspath(__file__),
            "server_addr": options["addr"],
            "server_connect_addr": connect_addr,
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
        """
        remote_state = launch_remote_processes(
            {
            "connect_addr": ps_scan_server_options["server_connect_addr"],
            "script_path": ps_scan_server_options["script_path"],
            "server_port": ps_scan_server_options["server_port"],
            "type": options.get("type"),
            "threads": options.get("threads"),
            "es_type": options.get("es_type"),
            "log": options.get("log"),
            },
            callback=remote_callback,
            node_list = ps_scan_server_options["node_list"],
        )

        if options.get("user"):
           try:
               become_user(options.get("user"))
           except Exception as e:
               LOG.exception(e)
               self.shutdown()
               return
        """
        setup_logger(options)
        LOG.debug({"msg": "Parsed options", "options": json.dumps(options, indent=2, sort_keys=True)})
        LOG.debug({"msg": "Initial scan paths", "paths": ", ".join(args)})
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
                for es_option_name in [
                    "es_bulk_refresh",
                    "es_flush_timeout",
                    "es_max_send_q_size",
                    "es_replicas",
                    "es_send_q_sleep",
                    "es_shards",
                    "es_threads",
                ]:
                    es_options[es_option_name] = options[es_option_name]
                ps_scan_server_options["client_config"]["es_options"] = es_options
                ps_scan_server_options["client_config"]["es_cmd_idx"] = es_cmd_idx
            if es_client and (options["es_init_index"] or options["es_reset_index"]):
                if options["es_reset_index"]:
                    elasticsearch_wrapper.es_delete_index(es_client, es_cmd_idx)
                LOG.debug({"msg": "Initializing indices for Elasticsearch", "index": es_options["index"]})
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
                final_stats = svr.get_stats()
                stop_options = elasticsearch_wrapper.es_create_stop_options(options, stats=final_stats)
                elasticsearch_wrapper.es_stop_processing(es_client, es_cmd_idx, stop_options, options)
        except Exception as e:
            LOG.exception({"msg": "Unhandled exception in server", "exception": str(e)})
            sys.exit(4)


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
