#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
PowerScale file scanner
"""
# fmt: off
__title__         = "ps_scan_api_server"
__version__       = "0.1.0"
__date__          = "25 September 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import gzip
import io
import json
import logging
from logging.config import dictConfig
import os
import re
import signal
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "libs"))


dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d][tid:%(threadName)s] %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "INFO", "handlers": ["wsgi"]},
    }
)

import helpers.cli_parser_api as cli_parser
from helpers.constants import *
import helpers.scanner as scanner
import helpers.misc as misc
from libs.flask import Flask
from libs.flask import make_response
from libs.flask import request
from libs.flask import Response
from libs.simple_cache import SimpleCache
from libs.waitress import create_server

try:
    from libs.onefs_become_user import become_user
except:
    pass

APP = Flask(__name__)
ENCODING_GZIP = "gzip"
ENCODING_DEFLATE = "deflate"
HTTP_HDR_ACCEPT_ENCODING = "Accept-Encoding"
HTTP_HDR_CONTENT_ENCODING = "Content-Encoding"
HTTP_HDR_CONTENT_LEN = "Content-Length"
JSON_SER_ERR = "<not serializable>"
LOG = APP.logger
MIME_TYPE_JSON = "application/json"
REQUIRED_RETURN_FIELDS = [x[0] for x in scanner.PSCALE_DISKOVER_FIELD_MAPPING]
SERVER = None
TXT_INVALID_TOKEN = "Invalid continuation token received. Either the token does not exist or the token has expired"
TXT_QUERY_PATH_REQUIRED = "A URL encoded path is required in the 'path' query parameter"
TXT_UNABLE_TO_SCAN_DIRECTORY = "Unable to scan directory"
TXT_UNABLE_TO_SCAN_FILE_OR_DIR = "Unable to scan file/directory"


def signal_handler(signum, frame):
    global SERVER
    if signum in [signal.SIGINT, signal.SIGTERM]:
        SERVER.close()
        # Cleanup SimpleCache
        cache = APP.config.get("cache")
        if cache:
            del APP.config["cache"]
            cache.__del__()
        sys.exit(0)
    if signum in [signal.SIGUSR1]:
        root_logger = logging.getLogger("")
        cur_level = root_logger.getEffectiveLevel()
        if cur_level != logging.DEBUG:
            root_logger.setLevel(logging.DEBUG)
        else:
            root_logger.setLevel(logging.INFO)
        LOG.critical(
            {
                "msg": "SIGUSR1 signal received. Toggling debug.",
                "prev_state": cur_level,
                "next_state": root_logger.getEffectiveLevel(),
            }
        )


@APP.after_request
def compress(response):
    # 0: No compression, 1: Fastest, 9: Slowest
    compress_level = 9
    accept_encoding = request.headers.get(HTTP_HDR_ACCEPT_ENCODING, "").lower()
    if (
        response.status_code < 200
        or response.status_code >= 300
        or response.direct_passthrough
        or ((ENCODING_GZIP not in accept_encoding) and (ENCODING_DEFLATE not in accept_encoding))
        or HTTP_HDR_CONTENT_ENCODING in response.headers
    ):
        return response
    # Prefer gzip over deflate when available
    if ENCODING_GZIP in accept_encoding:
        buffer = io.BytesIO()
        with gzip.GzipFile(mode="wb", compresslevel=compress_level, fileobj=buffer) as gz_file:
            gz_file.write(response.get_data())
        content = buffer.getvalue()
        encoding = ENCODING_GZIP
    elif ENCODING_DEFLATE in accept_encoding:
        content = gzip.zlib.compress(response.get_data(), compress_level)
        encoding = ENCODING_DEFLATE
    response.set_data(content)
    response.headers[HTTP_HDR_CONTENT_LEN] = len(content)
    response.headers[HTTP_HDR_CONTENT_ENCODING] = encoding
    return response


@APP.route("/cluster_storage_stats", methods=["GET"])
def handle_cluster_storage_stats():
    args = request.args
    storage_usage_stats = {}
    if misc.is_onefs_os():
        storage_usage_stats = misc.get_local_storage_usage_stats()
    else:
        # TODO: Add support for querying for usage statistics
        pass
    resp = Response(json.dumps(storage_usage_stats), mimetype=MIME_TYPE_JSON)
    return resp


@APP.route("/ps_stat/list", methods=["GET"])
def handle_ps_stat_list():
    """Returns metadata for both the root path and all the immediate children of that path
    In the case the root is a file, only the file metadata will be returned

    Query arguments (common)
    ----------
    fields: <string> Comma separated string with a complete list of field names to return. Defaults to the empty string
            which returns all fields.
    limit: <int> Maximum number of entries to return in a single call. Defaults to 10000. Maximum value of 100000
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    token: <string> Token string to allow the continuation of a previous scan request when that request did not return
            all the available data for a specific root path. Tokens expire and using an expired token results in a 404
    type: <string> Type of scan result to return. One of: powerscale|diskover. The default is powerscale.

    Query arguments (optional)
    ----------
    custom_tagging: <bool> When true call a custom handler for each file. Enabling this can slow down scan speed
    extra_attr: <bool> When true, gets extra OneFS metadata. Enabling this can slow down scan speed
    include_root: <bool> When true, the metadata for the path specified in the path query parameter will be returned
            in the "contents" object under the key "root"
    no_acl: <bool> When true, skip ACL parsing. Enabling this can speed up scanning but results will not have ACLs
    no_names: <bool> When true, skip UIG/GID/SID to name translation. Enabling this can speed up scanning
    strip_dot_snapshot: <bool> When true, strip the .snapshot name from the file path returned
    user_attr: <bool> # When true, get user attribute data for files. Enabling this can slow down scan speed

    A bool value is false if the value is 0 or the string false. Any other value is interpreted as a true value.

    Returns
    ----------
    dict - A dictionary representing the root and files scanned
        {
          "contents": {
            "dirs": [<dict>]                  # List of directory metadata objects
            "files": [<dict>]                 # List of file metadata objects
            "root": <dict>                    # Metadata object for the root path
          }
          "items_total": <int>                # Total number of items remaining that could be returned
          "items_returned": <int>             # Number of metadata items returned. This number includes the "root"
          "token_continuation": <string>      # String that should be used in the "token" query argument to continue
                                              # scanning a directory
          "token_expiration": <int>           # Epoch seconds specifying when the token will expire
          "statistics": {
            "lstat_required": <bool>          # Number of times lstat was called vs. internal stat call
            "not_found": <int>                # Number of files that were not found
            "processed": <int>                # Number of files actually processed
            "skipped": <int>                  # Number of files skipped
            "time_access_time": <int>         # Seconds spent getting the file access time
            "time_acl": <int>                 # Seconds spent getting file ACL
            "time_custom_tagging": <int>      # Seconds spent processing custom tags
            "time_data_save": <int>           # Seconds spent creating response
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_filter": <int>              # Seconds spent filtering fields
            "time_open": <int>                # Seconds spent in os.open
            "time_name": <int>                # Seconds spend translating UID/GID/SID to names
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }
    """
    args = request.args
    options = APP.config["ps_scan"]["options"]
    param = {
        "custom_tagging": misc.parse_arg_bool(args, "custom_tagging", False),
        "extra_attr": misc.parse_arg_bool(args, "extra_attr", False),
        "include_root": misc.parse_arg_bool(args, "include_root", False),
        "fields": str(args.get("fields", "")),
        "limit": misc.parse_arg_int(args, "limit", options["default_item_limit"], 1, options["max_item_limit"]),
        "no_acl": misc.parse_arg_bool(args, "no_acl", False),
        "no_names": misc.parse_arg_bool(args, "no_names", False),
        "nodepool_translation": APP.config["ps_scan"]["nodepool_translation"],
        "path": args.get("path"),
        "strip_do_snapshot": misc.parse_arg_bool(args, "strip_dot_snapshot", True),
        "token": args.get("token"),
        "type": args.get("type", DEFAULT_DATA_TYPE),
        "user_attr": misc.parse_arg_bool(args, "user_attr", False),
    }
    if not param["path"]:
        return make_response({"msg": TXT_QUERY_PATH_REQUIRED}, 404)
    if param["fields"]:
        # Parse fields to return
        fields = param["fields"].split(",")
        param["fields"] = []
        for field in fields:
            if not re.match(r"[^a-zA-Z0-9_\-,.]", field):
                param["fields"].append(field)
        # Ensure we have a minimum set of fields
        if param["fields"]:
            param["fields"] = list(set(param["fields"] + REQUIRED_RETURN_FIELDS))

    dir_list = []
    dir_list_len = 0
    list_stat_data = {}
    root = {}
    root_is_dir = False
    stat_data = None
    time_list_dir = 0
    token_continuation = ""
    token_expiration = ""

    # Get the base directory, the last path component, and the full path. e.g. /ifs, foo, /ifs/foo
    base, file, full_path = misc.get_path_from_urlencoded(param["path"])
    if not param["token"]:
        if param["include_root"]:
            stat_data = scanner.file_handler_pscale(base, [file], param)
            if stat_data["dirs"]:
                root = stat_data["dirs"][0]
                root_is_dir = True
                param["limit"] -= 1
            elif stat_data["files"]:
                root = stat_data["files"][0]
                param["limit"] -= 1
        else:
            root_is_dir = True

    if root_is_dir or param["token"]:
        start = time.time()
        # Get the list of files/directories to process either from the file system directly or a cache
        if param["token"]:
            # Get the cached list and then set dir_list
            LOG.debug({"msg": "Getting cached directory listing", "path": full_path})
            cached_item = APP.config["cache"].get_item(param["token"])
            if not cached_item:
                return make_response({"msg": TXT_INVALID_TOKEN, "token": param["token"]}, 404)
            base = cached_item["base"]
            dir_list = cached_item["dir_list"]
            offset = cached_item["offset"] + param["limit"]
            LOG.debug({"msg": "Cached directory listing complete", "path": full_path, "length": cached_item["length"]})
        else:
            # Process the direct children of the passed in path
            LOG.debug({"msg": "Getting directory listing", "path": full_path})
            dir_list = misc.get_directory_listing(full_path)
            LOG.debug({"msg": "Directory listing complete", "path": full_path})
            offset = param["limit"]
            if dir_list is None:
                return make_response({"msg": TXT_UNABLE_TO_SCAN_DIRECTORY, "path": full_path}, 404)
        time_list_dir = time.time() - start
        # Split this list up into chunks dependent on the 'limit' query value
        dir_list_len = len(dir_list)
        if dir_list_len > param["limit"]:
            start - time.time()
            # Cache the remainder of the directory listing to avoid re-scanning the directory
            token_data = APP.config["cache"].add_item(
                {
                    "base": full_path,
                    "dir_list": dir_list[param["limit"] :],
                    "length": dir_list_len - param["limit"],
                    "offset": offset,
                }
            )
            dir_list = dir_list[0 : param["limit"]]
            token_continuation = token_data["token"]
            token_expiration = token_data["expiration"]
            LOG.debug({"msg": "Caching directory listing", "time": time.time() - start})
        # Perform the actual stat commands on each file/directory
        LOG.debug({"msg": "Parsing directory", "path": full_path})
        list_stat_data = scanner.file_handler_pscale(full_path, dir_list, param)
        LOG.debug({"msg": "Parsing complete", "path": full_path, "stats": list_stat_data["statistics"]})

    # Calculate statistics to return in the response
    dirs_len = len(list_stat_data.get("dirs", []))
    files_len = len(list_stat_data.get("files", []))
    items_total = dir_list_len + 1 * (not not root)
    items_returned = dirs_len + files_len + 1 * (not not root)
    total_stats = {}
    if stat_data:
        if list_stat_data:
            for key in list_stat_data["statistics"]:
                total_stats[key] = list_stat_data["statistics"][key] + stat_data["statistics"][key]
        else:
            total_stats = stat_data.get("statistics", {})
    else:
        total_stats = list_stat_data.get("statistics", {})
    total_stats["time_list_dir"] = time_list_dir

    # Build response
    resp_data = {
        "contents": {
            "dirs": list_stat_data.get("dirs", []),
            "files": list_stat_data.get("files", []),
            "root": root,
        },
        "items_total": items_total,
        "items_returned": items_returned,
        "token_continuation": token_continuation,
        "token_expiration": token_expiration,
        "statistics": total_stats,
    }
    if param["type"] == DATA_TYPE_DISKOVER:
        scanner.convert_response_to_diskover(resp_data)
    return Response(json.dumps(resp_data, default=lambda o: JSON_SER_ERR), mimetype=MIME_TYPE_JSON)


@APP.route("/ps_stat/single", methods=["GET"])
def handle_ps_stat_single():
    """Returns metadata for a single file or directory specified by the "path" argument

    Query arguments (common)
    ----------
    fields: <string> Comma separated string with a complete list of field names to return. Defaults to the empty string
            which returns all fields.
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    type: <string> Type of scan result to return. One of: powerscale|diskover. The default is powerscale.

    Query arguments (optional)
    ----------
    custom_tagging: <bool> When true call a custom handler for each file. Enabling this can slow down scan speed
    extra_attr: <bool> When true, gets extra OneFS metadata. Enabling this can slow down scan speed
    no_acl: <bool> When true, skip ACL parsing. Enabling this can speed up scanning but results will not have ACLs
    no_names: <bool> When true, skip UIG/GID/SID to name translation. Enabling this can speed up scanning
    strip_dot_snapshot: <bool> When true, strip the .snapshot name from the file path returned
    user_attr: <bool> # When true, get user attribute data for files. Enabling this can slow down scan speed

    Returns
    ----------
    dict - A dictionary representing the single item scanned
        {
          "contents": {
            "dirs": []                        # Empty list
            "files": []                       # Empty list
            "root": <dict>                    # Metadata for the root path
          }
          "items_total": <int>                # Total number of items remaining that could be returned
          "items_returned": <int>             # Number of metadata items returned. This number includes the "root"
          "statistics": {
            "lstat_required": <bool>          # Number of times lstat was called vs. internal stat call
            "not_found": <int>                # Number of files that were not found
            "processed": <int>                # Number of files actually processed
            "skipped": <int>                  # Number of files skipped
            "time_access_time": <int>         # Seconds spent getting the file access time
            "time_acl": <int>                 # Seconds spent getting file ACL
            "time_custom_tagging": <int>      # Seconds spent processing custom tags
            "time_data_save": <int>           # Seconds spent creating response
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_filter": <int>              # Seconds spent filtering fields
            "time_name": <int>                # Seconds spend translating UID/GID/SID to names
            "time_open": <int>                # Seconds spent in os.open
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }
    """
    args = request.args
    param = {
        "custom_tagging": misc.parse_arg_bool(args, "custom_tagging", False),
        "extra_attr": misc.parse_arg_bool(args, "extra_attr", False),
        "fields": str(args.get("fields", "")),
        "no_acl": misc.parse_arg_bool(args, "no_acl", False),
        "no_names": misc.parse_arg_bool(args, "no_names", False),
        "nodepool_translation": APP.config["ps_scan"]["nodepool_translation"],
        "path": args.get("path"),
        "strip_do_snapshot": misc.parse_arg_bool(args, "strip_dot_snapshot", True),
        "type": args.get("type", DEFAULT_DATA_TYPE),
        "user_attr": misc.parse_arg_bool(args, "user_attr", False),
    }
    if not param["path"]:
        return make_response({"msg": TXT_QUERY_PATH_REQUIRED}, 404)
    if param["fields"]:
        # Parse fields to return
        fields = param["fields"].split(",")
        param["fields"] = []
        for field in fields:
            if not re.match(r"[^a-zA-Z0-9_\-,.]", field):
                param["fields"].append(field)
        # Ensure we have a minimum set of fields
        if param["fields"]:
            param["fields"] = list(set(param["fields"] + REQUIRED_RETURN_FIELDS))

    # Get the base directory, the last path component, and the full path. e.g. /ifs, foo, /ifs/foo
    base, file, full_path = misc.get_path_from_urlencoded(param["path"])
    stat_data = scanner.file_handler_pscale(base, [file], param)
    if stat_data["dirs"]:
        root = stat_data["dirs"][0]
    elif stat_data["files"]:
        root = stat_data["files"][0]
    else:
        root = {}
    if not root:
        return make_response({"msg": TXT_UNABLE_TO_SCAN_FILE_OR_DIR, "path": full_path}, 404)
    resp_data = {
        "contents": {
            "dirs": [],
            "files": [],
            "root": root,
        },
        "items_total": 1,
        "items_returned": 1 * (not not root),
        "statistics": stat_data["statistics"],
    }
    if param["type"] == DATA_TYPE_DISKOVER:
        scanner.convert_response_to_diskover(resp_data)
    return Response(json.dumps(resp_data, default=lambda o: JSON_SER_ERR), mimetype=MIME_TYPE_JSON)


if __name__ == "__main__" or __file__ == None:
    # Setup command line parser and parse agruments
    (parser, options, args) = cli_parser.parse_cli(sys.argv, __version__, __date__)
    if options["debug"]:
        LOG.setLevel(logging.DEBUG)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGUSR1, signal_handler)
    if misc.is_onefs_os():
        # Set resource limits
        old_limit, new_limit = misc.set_resource_limits(options["ulimit_memory"])
        if new_limit:
            LOG.debug({"msg": "VMEM ulimit value set", "new_value": new_limit})
        else:
            LOG.info({"msg": "VMEM ulimit setting failed", "mem_size": options["ulimit_memory"]})

    # Turn of serialization of cache items into JSON
    options.update({"serialize": False})
    APP.config["cache"] = SimpleCache(options)
    APP.config["ps_scan"] = {
        "options": options,
        "nodepool_translation": misc.get_nodepool_translation(),
    }

    if options["user"]:
        try:
            become_user(options["user"])
        except Exception as e:
            LOG.exception(e)
            sys.exit(1)

    svr_addr = "*" if options["addr"] == DEFAULT_SERVER_ADDR else options["addr"]
    SERVER = create_server(
        APP,
        host=svr_addr,
        ident="ps_scan_api_server/{ver}".format(ver=__version__),
        port=options["port"],
        threads=options["threads"],
    )
    SERVER.run()

    # Cleanup SimpleCache
    cache = APP.config.get("cache")
    if cache:
        del APP.config["cache"]
        cache.__del__()
