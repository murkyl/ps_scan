#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "ps_scan_cli_parser"
__version__       = "1.0.0"
__date__          = "20 March 2023"
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

import scanit
from helpers.constants import *

USAGE = "usage: %prog [OPTION...] PATH... [PATH..]"
EPILOG = """
Quickstart
====================
The --es-cred-file or the --es-url and optionally both --es-user and --es-pass need to
be present for the script to send output to an Elasticsearch endpoint.

The --es-cred-file is a credentials file that contains up to 4 lines of data.
The first line is the user name
The second is the password
The third, optional line is the index
The fourth, optional line is the URL

If you specify the URL you must specify the index as well.
This es-cred-file is sensitive and should be properly secured.

Command line options
Some options can significantly reduce scan speed. The following options may cause scan
speeds to be reduced by more than half:
  * extra
  * tagging
  * user-attr

Custom tagging file format
====================
TBD
"""

def add_parser_options(parser):
    parser.add_option(
        "-t",
        "--type",
        type="choice",
        choices=("basic", "onefs"),
        default=None,
        help="""Scan type to use.                                     
basic: Works on all file systems.                     
onefs: Works on OneFS based file systems.             
""",
    )
    parser.add_option(
        "--extra",
        action="store_true",
        default=False,
        help="Add additional file information on OneFS systems",
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
        default=False,
        help="Retrieve user defined extended attributes from each file",
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
    group.add_option(
        "--threads-per-proc",
        action="store",
        type="int",
        default=DEFAULT_THREADS_PER_PROC_COUNT,
        help="""File scan threads per process.                        
Number of processes = Threads / ThreadsPerProc        
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
        help="""
Prefix of Elasticsearch index. The suffixes _file and _dir will automatically be appended to this prefix
""",
    )
    group.add_option(
        "--es-init-index",
        action="store_true",
        default=False,
        help="When set, the script will initialize the index before uploading data",
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
        "--es-cred-file",
        action="store",
        default=None,
        help="File holding the user name and password, on individual lines, for Elasticsearch",
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
        "--es-max-send-q-size",
        action="store",
        type="int",
        default=DEFAULT_MAX_Q_SIZE,
        help="""Number of unsent entries in the Elasticsearch send    
queue before throttling file scanning.                
Default: %default
""",
    )
    group.add_option(
        "--es-send-q-sleep",
        action="store",
        type="int",
        default=DEFAULT_SEND_Q_SLEEP,
        help="""When max send queue size is reached, sleep each file  
scanner by this value in seconds to slow scanning.    
Default: %default
""",
    )
    parser.add_option_group(group)
    group = optparse.OptionGroup(parser, "Logging and debug options")
    group.add_option(
        "--log",
        default=None,
        help="Full path and file name for log output.  If not set, no log output to file will be generated",
    )
    group.add_option(
        "--console-log",
        action="store_true",
        default=False,
        help="When this flag is set, log output to console",
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


def add_parser_options_advanced(parser):
    group = optparse.OptionGroup(parser, "ADVANCED options")
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
        "--q-poll-interval",
        action="store",
        type="int",
        default=scanit.DEFAULT_POLL_INTERVAL,
        help="""Number of seconds to wait in between polling events   
for the statistics and ES send loop.                  
Default: %default
""",
    )
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
    if "--advanced" in argv:
        add_parser_options_advanced(parser)
    (options, args) = parser.parse_args(argv[1:])
    return (parser, options, args)