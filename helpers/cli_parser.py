#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "ps_scan_cli_parser"
__version__       = "1.0.0"
__date__          = "10 April 2023"
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

import helpers.scanit as scanit
from helpers.constants import *

USAGE = "usage: %prog [OPTION...] PATH... [PATH..]"
EPILOG = """
Description
====================
The path parameter(s) to specify the scan paths supports standard UNIX globbing.
e.g. /ifs/path/dir*

Scans can be done against a snapshot. The script will automatically strip the
.snapshot/<snapshotname> portion of the path away when it is present.


Quickstart
====================
Run on a cluster:
python ps_scan.py --es-cred=credentials.txt --es-reset-index --nodes=1-3 /ifs

This command will cause the script to do the following:
  * Read ElasticSearch credentials from the file credentials.txt
  * Clear any old ES index and create a new ES index
  * Run 1 scanner process on nodes 1, 2, and 3
  * Start scanning from the path: /ifs

The --es-cred-file or the --es-url, the --es-index, --es-user and --es-pass need to
be present for the script to send output to an Elasticsearch endpoint.

The --es-cred-file is a credentials file that contains up to 4 lines of data.
The first line is the user name
The second is the password
The third, optional line is the index
The fourth, optional line is the URL

If you specify the URL you must specify the index as well.
This es-cred-file is sensitive information and should be properly secured. Remove read
permissions for any user that should not have access to this file.


Command line options
====================
Some options can significantly impact scan speed. The following options may cause scan
speeds to impacted:
  * --no-acl
      When this option is selected, the script will not attempt to parse ACL information
      when running on a OneFS system. This will result in any files that have a user or
      group owner that is a SID to instead use the UNIX equivalent value.
      
      This option can improve scan speeds at the expense of missing ACL information.
  * --extra
      When this option is selected, some additional metadata items will be present in
      the output. These fields include:
      file_coalescer
      file_is_manual_access
      file_is_manual_packing
      file_is_manual_protection
      size_metadata
      
      This option can decrease scan speeds.
  * --user-attr
      When this option is enabled, extended attributes stored in the user portion of the
      file will be scanned.
      
      This option can decrease scan speeds.


Logging
====================
You can dynamically enable and disable debugging by sending a SIGUSR1 to the main
process.

Example on a Bash shell where <pid> is replaced with the actual process ID:
kill -SIGUSR1 <pid>

Sending a SIGUSR2 to the coordinator process will cause all scanners to dump state to
the configured log output.


Return values
====================
0   No errors
1   Error parsing the command line
2   Parse type set to OneFS but script is not run on a OneFS operating systems
3   Error opening or parsing the Elasticsearch credential file
"""


def add_parser_options(parser):
    parser.add_option(
        "--op",
        type="choice",
        choices=(OPERATION_TYPE_AUTO, OPERATION_TYPE_CLIENT, OPERATION_TYPE_SERVER),
        default=OPERATION_TYPE_AUTO,
        help="""Scan type to use. Defaults to: auto                   
auto: Automatically launch server and clients         
server: Launch a server instance.                     
client: Launch a client to connect to a server.       
""",
    )
    parser.add_option(
        "-t",
        "--type",
        type="choice",
        choices=(SCAN_TYPE_AUTO, SCAN_TYPE_BASIC, SCAN_TYPE_ONEFS),
        default=SCAN_TYPE_AUTO,
        help="""Scan type to use. Defaults to: auto                   
auto: Use onefs when possible and fallback to basic   
basic: Works on all file systems.                     
onefs: Works on OneFS based file systems.             
""",
    ),
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
        "--no-acl",
        action="store_true",
        default=DEFAULT_PARSE_SKIP_ACLS,
        help="Skip parsing ACL file permissions on OneFS systems",
    )
    parser.add_option(
        "--no-names",
        action="store_true",
        default=DEFAULT_PARSE_SKIP_NAMES,
        help="Skip translation of UID/GID/SID into names on OneFS systems",
    )
    parser.add_option(
        "--extra",
        action="store_true",
        default=DEFAULT_PARSE_EXTRA_ATTR,
        help="Parse additional file metadata information on OneFS systems",
    )
    # parser.add_option(
    #    "--tagging",
    #    action="store",
    #    default=None,
    #    help="Turn on custom tagging based on tagging rules specified in the file. See documentation for file format",
    # )
    parser.add_option(
        "--user-attr",
        action="store_true",
        default=DEFAULT_PARSE_USER_ATTR,
        help="Parse user defined extended attributes for each file on OneFS systems",
    )
    parser.add_option(
        "--stats-interval",
        action="store",
        type="int",
        default=DEFAULT_STATS_OUTPUT_INTERVAL,
        help="""Stats update interval in seconds.                     
Default: %default
""",
    )
    group = optparse.OptionGroup(parser, "Performance options")
    group.add_option(
        "--threads",
        action="store",
        type="int",
        default=DEFAULT_THREAD_COUNT,
        help="""Number of file scanning threads.                      
Default: %default
""",
    )
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Elasticsearch options")
    group.add_option(
        "--es-url",
        action="store",
        default=None,
        help="Full URL to Elasticsearch endpoint",
    )
    group.add_option(
        "--es-index",
        action="store",
        default=None,
        help="""Prefix of Elasticsearch index. The suffixes _file and _dir  
will automatically be appended to this prefix         
""",
    )
    group.add_option(
        "--es-type",
        type="choice",
        choices=(ES_TYPE_PS_SCAN, ES_TYPE_DISKOVER),
        default=None,
        help="""ElasticSearch endpoint type. Options: ps_scan, diskover     
Default: ps_scan                                      
""",
    )
    group.add_option(
        "--es-user",
        action="store",
        default=None,
        help="Elasticsearch user",
    )
    group.add_option(
        "--es-pass",
        action="store",
        default=None,
        help="Elasticsearch password",
    )
    group.add_option(
        "--es-options-file",
        action="store",
        default=None,
        help="""File holding at a minimum the user name and password, 
on individual lines, for Elasticsearch. Additionally  
you can specify the index name and the URL for the    
Elasticsearch on the following 2 lines.               
""",
    )
    group.add_option(
        "--es-cred-file",
        dest="es_options_file",
        action="store",
        default=None,
        help="""Deprecated option. Alias for --es-options-file.       
""",
    )
    group.add_option(
        "--es-init-index",
        action="store_true",
        default=False,
        help="When set, the script will initialize the index before uploading data",
    )
    group.add_option(
        "--es-reset-index",
        action="store_true",
        default=False,
        help="""When set, the script will delete any existing indices 
before creating a new one. This option implies the    
--es-init-index option is also set                    
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
        "--log",
        default=None,
        help="Full path and file name for log output.  If not set, no log output to file will be generated",
    )
    group.add_option(
        "--quiet",
        action="store_true",
        default=False,
        help="When this flag is set, do not log output to console",
    )
    group.add_option(
        "--debug",
        default=0,
        action="count",
        help="Add multiple debug flags to increase debug",
    )
    parser.add_option_group(group)


def add_parser_options_advanced(parser, hide_options=False):
    group = optparse.OptionGroup(parser, "ADVANCED options")
    group.add_option(
        "--cmd-poll-interval",
        action="store",
        type="float",
        default=DEFAULT_CMD_POLL_INTERVAL,
        help="""Number of whole seconds between directory queue size  
updates from a subprocess to the coordinator.         
Default: %default
""",
    )
    group.add_option(
        "--compression",
        type="choice",
        choices=[str(x) for x in list(range(0, 10))],
        default=DEFAULT_COMPRESSION_LEVEL,
        help="""Compression level used when sending results to the    
ElasticSearch instance. Valid values are 0-9 with 0   
being no compression and 9 being maximum compression  
Default: %default
""",
    )
    group.add_option(
        "--dir-output-interval",
        action="store",
        type="int",
        default=DEFAULT_DIR_OUTPUT_INTERVAL,
        help="""Number of whole seconds between directory queue size  
updates from a subprocess to the coordinator.         
Default: %default
""",
    )
    group.add_option(
        "--dir-request-interval",
        action="store",
        type="int",
        default=DEFAULT_DIR_REQUEST_INTERVAL,
        help="""Number of whole seconds between requests for          
additional work directories by a subprocess.          
Default: %default
""",
    )
    group.add_option(
        "--dirq-chunk",
        action="store",
        type="int",
        default=scanit.DEFAULT_QUEUE_DIR_CHUNK_SIZE,
        help="""Number of directories to put into each work chunk.    
Default: %default
""",
    )
    group.add_option(
        "--dirq-priority",
        action="store",
        type="int",
        default=scanit.DEFAULT_DIR_PRIORITY_COUNT,
        help="""Number of threads that are biased to process          
directories.                                          
Default: %default
""",
    )
    group.add_option(
        "--dirq-request-percentage",
        action="store",
        type="float",
        default=DEFAULT_DIRQ_REQUEST_PERCENTAGE,
        help="""Percentage of the number of unprocessed directory     
chunks to return each time a process requests work.   
Default: %default
""",
    )
    group.add_option(
        "--es-bulk-refresh",
        action="store",
        default=DEFAULT_ES_BULK_REFRESH_INTERVAL,
        help="""ElasticSearch time string the specifies how much time 
between indexing data and when it will be available.  
Default: %default
""",
    )
    group.add_option(
        "--es-max-send-q-size",
        action="store",
        type="int",
        default=DEFAULT_ES_MAX_Q_SIZE,
        help="""Number of unsent entries in the Elasticsearch send    
queue before throttling file scanning.                
Default: %default
""",
    )
    group.add_option(
        "--es-send-q-sleep",
        action="store",
        type="int",
        default=DEFAULT_ES_SEND_Q_SLEEP,
        help="""When max send queue size is reached, sleep each file  
scanner by this value in seconds to slow scanning.    
Default: %default
""",
    )
    group.add_option(
        "--es-shards",
        action="store",
        type="int",
        default=DEFAULT_ES_SHARDS,
        help="""Number of threads to send data to Elasticsearch.      
Default: %default
""",
    )
    group.add_option(
        "--es-replicas",
        action="store",
        type="int",
        default=DEFAULT_ES_REPLICAS,
        help="""Number of replications in Elasticsearch.
Default: %default
""",
    )
    group.add_option(
        "--es-threads",
        action="store",
        type="int",
        default=DEFAULT_ES_THREADS,
        help="""Number of threads to send data to Elasticsearch.      
Default: %default
""",
    )
    group.add_option(
        "--fileq-chunk",
        action="store",
        type="int",
        default=scanit.DEFAULT_QUEUE_FILE_CHUNK_SIZE,
        help="""Number of files to put into each work chunk.          
Default: %default
""",
    )
    group.add_option(
        "--fileq-cutoff",
        action="store",
        type="int",
        default=scanit.DEFAULT_FILE_QUEUE_CUTOFF,
        help="""When the number of files in the file queue is less    
than this value, bias threads to process directories. 
Default: %default
""",
    )
    group.add_option(
        "--fileq-min-cutoff",
        action="store",
        type="int",
        default=scanit.DEFAULT_FILE_QUEUE_MIN_CUTOFF,
        help="""When the number of files in the file queue is less    
than this value, only process directories if possible.
Default: %default
""",
    )
    group.add_option(
        "--max-work-items",
        action="store",
        type="int",
        default=DEFAULT_MAX_WORK_ITEMS_PER_REQUEST,
        help="""Maximum number of messages to include in a single work
item request call between the client and server.      
Default: %default
""",
    )
    group.add_option(
        "--request-work-interval",
        action="store",
        type="int",
        default=DEFAULT_REQUEST_WORK_INTERVAL,
        help="""Minimum number of second between process work requests
before more work can be sent to a process.            
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
default is 1 MiB which is insufficient for wide dirs.
Default: %default
""",
    )
    group.add_option(
        "--ulimit-memory-min",
        action="store",
        type="int",
        default=DEFAULT_ULIMIT_MEMORY_MIN,
        help="""When running on a OneFS system, the minimum amount of 
physical memory that must be present before increasing
the per process memory limit.                        
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
    # Fix up compression option
    raw_options.compression = int(raw_options.compression)
    return (parser, raw_options.__dict__, args)
