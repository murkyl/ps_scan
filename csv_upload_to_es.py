#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Take the CSV output from the ps_scan script and upload the data into an ElasticSearch index
"""
# fmt: off
__title__         = "csv_upload_to_es.py"
__version__       = "1.1.0"
__date__          = "02 April 2024"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import csv
import glob
import json
import logging
import optparse
import os
import platform
import queue
import sys

from helpers.constants import *
import helpers.elasticsearch_wrapper as elasticsearch_wrapper
import helpers.misc as misc
import helpers.scanner as scanner


DEFAULT_CHUNK_SIZE = 500
DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - %(message)s"
LOG = logging.getLogger()
USAGE = "usage: %prog [OPTION...] FILE1 [FILE2..]"
EPILOG = """
Description
====================
The CSV file parameter(s) to specify the data to upload supports standard UNIX globbing.
e.g. data-*.csv

"""


def expand_glob_list(args):
    expanded_list = []
    for arg in args:
        expanded_list.extend(glob.glob(arg))
    return expanded_list


def parse_cli(argv, prog_ver, prog_date):
    # Create our command line parser. We use the older optparse library for compatibility on OneFS
    optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = optparse.OptionParser(
        usage=USAGE,
        version="%prog v" + prog_ver + " (" + prog_date + ")",
        epilog=EPILOG,
    )

    group = optparse.OptionGroup(parser, "Elasticsearch output options")
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
        "--es-shards",
        action="store",
        type="int",
        default=DEFAULT_ES_SHARDS,
        help="""Number of CSV entries to send in a single bulk update.
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
        "--es-bulk-chunk",
        action="store",
        type="int",
        default=DEFAULT_CHUNK_SIZE,
        help="""Number of replications in Elasticsearch.
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

    (raw_options, args) = parser.parse_args(argv[1:])
    return (parser, raw_options.__dict__, args)


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


def flush_json_data_to_es(json_array, es_client, es_cmd_idx):
    dir_list = []
    file_list = []
    uploaded = 0
    for item in json_array:
        if item.get("file_type") == "file":
            file_list.append(item)
        elif item.get("file_type") == "dir":
            dir_list.append(item)
    if file_list:
        uploaded += elasticsearch_wrapper.es_data_flush([[CMD_SEND, file_list]], es_client, es_cmd_idx)
    if dir_list:
        uploaded += elasticsearch_wrapper.es_data_flush([[CMD_SEND_DIR, dir_list]], es_client, es_cmd_idx)
    return uploaded


def upload_csv_files(csv_file_list, es_client, es_cmd_idx, max_bulk_rows = DEFAULT_CHUNK_SIZE):
    total_rows = 0
    uploaded_rows = 0
    for file in csv_file_list:
        bulk_rows = []
        LOG.info("Starting upload of file: %s"%file)
        skip_header = True
        row_count = 0
        try:
            csvfile_handle = open(file, "r", newline="")
        except:
            try:
                csvfile_handle = open(file, "rb")
            except:
                LOG.critical("Unable to open file: %s"%file)
                continue
        data_reader = csv.reader(csvfile_handle)
        for row in data_reader:
            if skip_header:
                skip_header = False
                continue
            response = scanner.convert_csv_to_response(row)
            bulk_rows.append(response)
            row_count += 1
            if row_count >= max_bulk_rows:
                uploaded = flush_json_data_to_es(bulk_rows, es_client, es_cmd_idx)
                total_rows += row_count
                uploaded_rows += uploaded
                bulk_rows = []
        if bulk_rows:
            uploaded = flush_json_data_to_es(bulk_rows, es_client, es_cmd_idx)
            total_rows += row_count
            uploaded_rows += uploaded
            bulk_rows = []
        csvfile_handle.close()
    return total_rows, uploaded_rows


def main():
    # Setup command line parser and parse agruments
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)
    setup_logger(options)
    
    es_client = None
    es_cmd_idx = None
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

    if not es_options:
        LOG.critical("")
        sys.exit(10)
    es_client, es_cmd_idx = elasticsearch_wrapper.es_create_connection(
        es_options["url"],
        es_options["user"],
        es_options["password"],
        es_options["index"],
        es_options["type"],
    )
    if not es_client:
        LOG.critical("Could not create ElasticSearch connection")
        sys.exit(11)
    if (options["es_init_index"] or options["es_reset_index"]):
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

    start_options = elasticsearch_wrapper.es_create_start_options(options)
    elasticsearch_wrapper.es_start_processing(es_client, es_cmd_idx, start_options, options)
    # Upload all the CSV data
    csv_file_list = expand_glob_list(args)
    LOG.debug("CSV file list: %s"%csv_file_list)
    if not csv_file_list:
        LOG.error("No CSV files found for input")
        sys.exit(12)
    total_rows,uploaded_rows = upload_csv_files(csv_file_list, es_client, es_cmd_idx, options["es_bulk_chunk"])
    LOG.info("Documents uploaded to Elastic: %s"%uploaded_rows)
    if uploaded_rows > 0 and uploaded_rows != total_rows:
        LOG.info("Documents failed to upload Elastic: %s"%(total_rows - uploaded_rows))
    stop_options = elasticsearch_wrapper.es_create_stop_options(options)
    elasticsearch_wrapper.es_stop_processing(es_client, es_cmd_idx, stop_options, options)


if __name__ == "__main__" or __file__ == None:
    main()
