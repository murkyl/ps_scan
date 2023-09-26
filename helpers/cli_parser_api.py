#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "cli_parser_api"
__version__       = "1.0.0"
__date__          = "25 September 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "add_parser_options",
    "add_parser_options_advanced",
    "parse_cli",
]
# fmt: on
import optparse

from helpers.constants import *
import libs.simple_cache

USAGE = "usage: %prog [OPTION...]"
EPILOG = """
Description
====================
Start a PowerStore Scan API server

This server will allow queries to return different PowerScale specific information that
is not available through the standard PAPI or RAN methods.

To improve server security, it is recommended to use the --user option to have the server run in the context of this
user. The user needs to have the following RBAC privileges:
Read - ISI_PRIV_IFS_BACKUP - Required to read files in the file system for indexing
Read - ISI_PRIV_LOGIN_PAPI - Required to access PAPI
Read - ISI_PRIV_STATISTICS - Required to monitor system performance for throttling


Endpoints
====================
/cluster_storage_stats
    Description
    ----------
    Returns a JSON document with some cluster storage statistics. The specific fields
    are detailed below.

    Query arguments
    ----------
    None

    Returns
    ----------
    dict - A dictionary representing the single item scanned
    {
        "ifs.bytes.avail": <int>              # Total bytes available
        "ifs.bytes.free": <int>               # Total bytes free (superset of available)
        "ifs.bytes.used": <int>               # Total bytes used
        "ifs.bytes.total": <int>              # Total writeable bytes
        "ifs.percent.avail": <float>          # Percent of storage available between 0 and 1
        "ifs.percent.free": <float>           # Percent of storage free between 0 and 1 (superset of available)
    }

/ps_stat/single
    Description
    ----------
    Returns metadata for a single file or directory specified by the "path" argument.

    Query arguments (common)
    ----------
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    type: <string> Type of scan result to return. One of: powerscale|diskover. The default is powerscale.

    Query arguments (optional)
    ----------
    custom_tagging: <bool> When true call a custom handler for each file. Enabling this can slow down scan speed
    extra_attr: <bool> When true, gets extra OneFS metadata. Enabling this can slow down scan speed
    no_acl: <bool> When true, skip ACL parsing. Enabling this can speed up scanning but results will not have ACLs
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
            "statistics": {
              "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
              "not_found": <int>              # Number of files that were not found
              "processed": <int>              # Number of files actually processed
              "skipped": <int>                # Number of files skipped
              "time_access_time": <int>       # Seconds spent getting the file access time
              "time_acl": <int>               # Seconds spent getting file ACL
              "time_custom_tagging": <int>    # Seconds spent processing custom tags
              "time_dinode": <int>            # Seconds spent getting OneFS metadata
              "time_extra_attr": <int>        # Seconds spent getting extra OneFS metadata
              "time_lstat": <int>             # Seconds spent in lstat
              "time_scan_dir": <int>          # Seconds spent scanning the entire directory
              "time_user_attr": <int>         # Seconds spent scanning user attributes
            }
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
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }

/ps_stat/list
    Description
    ----------
    Returns metadata for both the root path and all the immediate children of that path.
    In the case the root is a file, only the file metadata will be returned.

    Query arguments (common)
    ----------
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    limit: <int> Maximum number of entries to return in a single call. Defaults to 10000. Maximum value of 100000
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
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }


Quickstart
====================
Run on a cluster:
python ps_scan_api_server.py


Return values
====================
0   No errors
1   CLI argument errors
"""


def add_parser_options(parser):
    parser.add_option(
        "--nodes",
        action="store",
        default="",
        help="""On a PowerScale cluster, the nodes that perform the   
actual file scan. This is a comma separated list of   
node numbers and node ranges. To run more than one    
process on a node, include the node multiple times.   
Examples:                                             
1,2,3,7,8,9
1-9
1-9,1-9
1,2,3,10-12,10-12,15,16,17
""",
    )
    parser.add_option(
        "--addr",
        action="store",
        default=DEFAULT_SERVER_ADDR,
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
        "--user",
        action="store",
        default=None,
        help="User in the current zone that the server will run as",
    )
    parser.add_option(
        "--default-item-limit",
        action="store",
        type="int",
        default=DEFAULT_ITEM_LIMIT,
        help="""Default number of items to return in a single request.
Default: %default
""",
    )
    group = optparse.OptionGroup(parser, "Performance options")
    group.add_option(
        "--threads",
        action="store",
        type="int",
        default=DEFAULT_THREAD_COUNT,
        help="""Number of HTTP server threads.                        
Default: %default
""",
    )
    group.add_option(
        "--advanced",
        action="store_true",
        default=False,
        help="Flag to enable advanced options",
    )
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Logging and debug options")
    group.add_option(
        "--debug",
        default=0,
        action="count",
        help="Add multiple debug flags to increase debug",
    )
    parser.add_option_group(group)


def add_parser_options_advanced(parser, hide_options=False):
    group = optparse.OptionGroup(parser, "ADVANCED options")
    parser.add_option(
        "--cache-size-max",
        action="store",
        type="int",
        default=libs.simple_cache.DEFAULT_CACHE_SIZE_MAX,
        help="""Size in bytes of the internal cache.                  
Default: %default
""",
    )
    parser.add_option(
        "--cache-cleanup-interval",
        action="store",
        type="int",
        default=libs.simple_cache.DEFAULT_CACHE_CLEANUP_INTERVAL,
        help="""How often the cache cleanup will run, in seconds.     
Default: %default
""",
    )
    parser.add_option(
        "--cache-timeout",
        action="store",
        type="int",
        default=libs.simple_cache.DEFAULT_CACHE_TIMEOUT,
        help="""Number of seconds that an entry in the cache will be  
valid before it is cleaned up.                        
Default: %default
""",
    )
    parser.add_option(
        "--max-item-limit",
        action="store",
        type="int",
        default=DEFAULT_ITEM_LIMIT_MAX,
        help="""Maximum number of items to return in a single request.
Default: %default
""",
    )
    group.add_option(
        "--ulimit-memory",
        action="store",
        type="int",
        default=DEFAULT_ULIMIT_MEMORY,
        help="""When running on a OneFS system, the amount of memory  
that each process will be allowed to utilize. The OS  
default is 2 GiB which is insufficient for wide dirs.
Default: %default
""",
    )
    if hide_options:
        for op in group.option_list:
            op.help = optparse.SUPPRESS_HELP
    parser.add_option_group(group)


def parse_cli(argv, prog_ver, prog_date):
    # Create our command line parser. We use the older optparse library for compatibility on OneFS
    optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = optparse.OptionParser(
        usage=USAGE,
        version="%prog v" + prog_ver + " (" + prog_date + ")",
        epilog=EPILOG,
    )
    add_parser_options(parser)
    add_parser_options_advanced(parser, ("--advanced" not in argv))
    (raw_options, args) = parser.parse_args(argv[1:])
    return (parser, raw_options.__dict__, args)
