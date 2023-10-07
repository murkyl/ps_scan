#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "es_wrapper"
__version__       = "1.0.0"
__date__          = "12 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "es_data_sender",
    "es_create_connection",
    "es_create_index_settings",
    "es_create_start_options",
    "es_create_stop_options",
    "es_delete_index",
    "es_init_index",
    "es_start_processing",
    "es_stop_processing",
]
# fmt: on
import copy
import json
import logging
import queue
import re
import socket
import threading
import time
import zlib

import libs.elasticsearch_lite as es_lite
import helpers.scanit as scanit
from helpers.constants import *
import helpers.misc as misc

ES_INDEX_MAPPING = {
    "properties": {
        # ========== Timestamps ==========
        # Time when this entry was scanned
        # "_scan_time": {"type": "date", "format": "epoch_second||date_optional_time"},
        # Last access time of the file both in fractional seconds and YYYY-mm-DD format
        "atime": {"type": "date", "format": "epoch_second||date_optional_time"},
        "atime_date": {"type": "date", "format": "date_optional_time||epoch_second"},
        # Birth/creation time of the file both in fractional seconds and YYYY-mm-DD format
        "btime": {"type": "date", "format": "epoch_second||date_optional_time"},
        "btime_date": {"type": "date", "format": "date_optional_time||epoch_second"},
        # Last metadata change time of the file both in fractional seconds and YYYY-mm-DD format
        "ctime": {"type": "date", "format": "epoch_second||date_optional_time"},
        "ctime_date": {"type": "date", "format": "date_optional_time||epoch_second"},
        # Last data modified time of the file both in fractional seconds and YYYY-mm-DD format
        "mtime": {"type": "date", "format": "epoch_second||date_optional_time"},
        "mtime_date": {"type": "date", "format": "date_optional_time||epoch_second"},
        # ========== Directory stats values ==========
        # Number of immediate subdirectories to this directory
        "dir_count_dirs": {"type": "long"},
        # Number of all subdirectories to this directory (recursive)
        "dir_count_dirs_recursive": {"type": "long"},
        # Number of files in the immediate directory
        "dir_count_files": {"type": "long"},
        # Number of files in this directory and all subdirectories (recursive)
        "dir_count_files_recursive": {"type": "long"},
        # Directory depth from the root
        "dir_depth": {"type": "long"},
        # Total size of all files in the immediate directory
        "dir_file_size": {"type": "long"},
        # Total size of all files in this directory (recursive)
        "dir_file_size_recursive": {"type": "long"},
        # Total physical size of all files in the immediate directory
        "dir_file_size_physical": {"type": "long"},
        # Total size of all files in this directory (recursive)
        "dir_file_size_physical_recursive": {"type": "long"},
        # Boolean flag set to true if this directory is a leaf with no subdirectories
        "dir_leaf": {"type": "boolean"},
        # ========== File and path strings ==========
        # Parent directory of the file
        "file_path": {"type": "keyword"},
        # Full file name of the file including the extension but without the path
        "file_name": {"type": "keyword"},
        # File name extension portion. This is generally the text after the last . in the file name
        "file_ext": {"type": "keyword"},
        # ========== File attributes ==========
        "file_access_pattern": {"type": "keyword"},
        "file_coalescer": {"type": "keyword"},
        "file_compression_ratio": {"type": "float"},
        # Number of hard links for the file. Files start with 1. A number > 1 indicates other links to the file
        "file_hard_links": {"type": "long"},
        # does the file contain any alternative data streams
        "file_is_ads": {"type": "boolean"},
        # is the file compressed
        "file_is_compressed": {"type": "boolean"},
        # is the file file dedupe disabled - (0: can dedupe, 1: do not dedupe)
        "file_is_dedupe_disabled": {"type": "boolean"},
        # is the file deduped, assume the file is fully/partially deduped if it has shadow store references
        "file_is_deduped": {"type": "boolean"},
        # is the file data stored int the inode
        "file_is_inlined": {"type": "boolean"},
        "file_is_manual_access": {"type": "boolean"},
        "file_is_manual_packing": {"type": "boolean"},
        "file_is_manual_protection": {"type": "boolean"},
        # is the file packed into a container (SFSE or Small File Storage Efficiency)
        "file_is_packed": {"type": "boolean"},
        # is the file a SmartLink or a stub file for CloudPools
        "file_is_smartlinked": {"type": "boolean"},
        # is the file a sparse file
        "file_is_sparse": {"type": "boolean"},
        # Type of the file object, typically "file", "dir", or "symlink". Values defined by the FILE_TYPE constant
        "file_type": {"type": "keyword"},
        # inode value of the file
        "inode": {"type": "long"},
        # Number of inodes this file has
        "inode_mirror_count": {"type": "byte"},
        # The inode number of the parent directory
        "inode_parent": {"type": "long"},
        # Number of times the inode has been modified. An indicator of file change rate. Starts at 2
        "inode_revision": {"type": "long"},
        # ========== Storage pool targets ==========
        "pool_target_data": {"type": "keyword"},
        "pool_target_data_name": {"type": "keyword"},
        "pool_target_metadata": {"type": "keyword"},
        "pool_target_metadata_name": {"type": "keyword"},
        # ========== Permissions ==========
        "perms_acl_aces": {"type": "keyword"},
        "perms_acl_group": {"type": "keyword"},
        "perms_acl_user": {"type": "keyword"},
        "perms_group": {"type": "keyword"},
        "perms_unix_bitmask": {"type": "short"},
        "perms_unix_gid": {"type": "long"},
        "perms_unix_uid": {"type": "long"},
        "perms_user": {"type": "keyword"},
        # ========== File protection level ==========
        "protection_current": {"type": "keyword"},
        "protection_target": {"type": "keyword"},
        # ========== File allocation size and blocks ==========
        # The apparent size of the file. Sparse files include the sparse area
        "size": {"type": "long"},
        # Logical size in 8K blocks. Sparse files only show the real data portion
        "size_logical": {"type": "long"},
        "size_metadata": {"type": "integer"},
        # Physical size on disk including protection overhead, including extension blocks and excluding metadata
        "size_physical": {"type": "long"},
        # Physical size on disk excluding protection overhead and excluding metadata
        "size_physical_data": {"type": "long"},
        # Physical size on disk of the protection overhead
        "size_protection": {"type": "long"},
        # ========== SSD usage ==========
        "ssd_strategy": {"type": "short"},
        "ssd_strategy_name": {"type": "keyword"},
        "ssd_status": {"type": "short"},
        "ssd_status_name": {"type": "keyword"},
        # ========== User attributes ==========
        "user_attributes": {"type": "object"},
        "user_tags": {"type": "keyword"},
    }
}
ES_INDEX_MAPPING_DISKOVER = {
    "properties": {
        "name": {
            "type": "keyword",
            "fields": {
                "text": {"type": "text", "analyzer": "filename_analyzer"},
            },
        },
        "parent_path": {
            "type": "keyword",
            "fields": {
                "text": {"type": "text", "analyzer": "path_analyzer"},
            },
        },
        "size": {"type": "long"},
        "size_norecurs": {"type": "long"},
        "size_du": {"type": "long"},
        "size_du_norecurs": {"type": "long"},
        "file_count": {"type": "long"},
        "file_count_norecurs": {"type": "long"},
        "dir_count": {"type": "long"},
        "dir_count_norecurs": {"type": "long"},
        "dir_depth": {"type": "integer"},
        "owner": {"type": "keyword"},
        "group": {"type": "keyword"},
        "mtime": {"type": "date"},
        "atime": {"type": "date"},
        "ctime": {"type": "date"},
        "nlink": {"type": "integer"},
        "ino": {"type": "keyword"},
        "tags": {"type": "keyword"},
        "costpergb": {
            "type": "scaled_float",
            "scaling_factor": 100,
        },
        "extension": {"type": "keyword"},
        "path": {"type": "keyword"},
        "total": {"type": "long"},
        "used": {"type": "long"},
        "free": {"type": "long"},
        "free_percent": {"type": "float"},
        "available": {"type": "long"},
        "available_percent": {"type": "float"},
        "file_size": {"type": "long"},
        "file_size_du": {"type": "long"},
        "file_count": {"type": "long"},
        "dir_count": {"type": "long"},
        "start_at": {"type": "date"},
        "end_at": {"type": "date"},
        "crawl_time": {"type": "float"},
        "diskover_ver": {"type": "keyword"},
        "type": {"type": "keyword"},
        "pscale": {
            "type": "object",
            "properties": copy.deepcopy(ES_INDEX_MAPPING["properties"]),
        },
    }
}
ES_INDEX_SETTINGS = {
    "number_of_shards": 1,
    "max_regex_length": 4096,
    "number_of_replicas": 0,
}
ES_INDEX_SETTINGS_DISKOVER = {
    "index": {
        "number_of_shards": 1,
        "number_of_replicas": 0,
        "codec": "default",
    },
    "analysis": {
        "tokenizer": {
            "filename_tokenizer": {
                "type": "char_group",
                "tokenize_on_chars": ["whitespace", "punctuation", "-", "_"],
            },
            "path_tokenizer": {
                "type": "char_group",
                "tokenize_on_chars": ["whitespace", "punctuation", "/", "-", "_"],
            },
        },
        "analyzer": {
            "filename_analyzer": {
                "tokenizer": "filename_tokenizer",
                "filter": ["word_filter", "lowercase"],
            },
            "path_analyzer": {
                "tokenizer": "path_tokenizer",
                "filter": ["word_filter", "lowercase"],
            },
        },
        "filter": {
            "word_filter": {
                "type": "word_delimiter_graph",
                "generate_number_parts": "false",
                "stem_english_possessive": "false",
                "split_on_numerics": "false",
                "catenate_all": "true",
                "preserve_original": "true",
            }
        },
    },
}
ES_REFERSH_INTERVAL = """{"index":{"refresh_interval":"%s"}}"""
LOG = logging.getLogger(__name__)


def es_create_connection(url, username, password, index, es_type=ES_TYPE_PS_SCAN):
    if es_type == ES_TYPE_PS_SCAN:
        if index[-1] == "_":
            index = index[0:-1]
        es_client = es_lite.ElasticsearchLite()
        es_client.username = username
        es_client.password = password
        es_client.endpoint = url
        return [es_client, {CMD_SEND: index + "_data", CMD_SEND_DIR: index + "_data", CMD_SEND_STATS: index + "_state"}]
    elif es_type == ES_TYPE_DISKOVER:
        es_client = es_lite.ElasticsearchLite()
        es_client.username = username
        es_client.password = password
        es_client.endpoint = url
        # Verify the index starts with diskover-
        if not index.startswith("diskover-"):
            index = "diskover-" + index
        # Append the current time if it is not passed in as the index name
        # The diskover index format is: diskover-<index_name>-YYYYmmddHHMM
        if not re.match(r"^diskover-(.*?)-[0-9]{12}$", index):
            index += "-" + time.strftime(DEFAULT_TIME_FORMAT_SIMPLE)
        return [es_client, {CMD_SEND: index, CMD_SEND_DIR: index, CMD_SEND_STATS: index}]
    else:
        raise Exception("Unknown ElasticSearch type: {es_type}. Could not create connection.".format(es_type=es_type))


def es_create_index_settings(options={}):
    new_settings = {}
    es_type = options.get("type")
    if es_type == ES_TYPE_PS_SCAN:
        new_settings["settings"] = copy.deepcopy(ES_INDEX_SETTINGS)
        new_settings["settings"]["number_of_shards"] = options.get(
            "number_of_shards", ES_INDEX_SETTINGS["number_of_shards"]
        )
        new_settings["settings"]["number_of_replicas"] = options.get(
            "number_of_replicas", ES_INDEX_SETTINGS["number_of_replicas"]
        )
        new_settings["mapping"] = copy.deepcopy(ES_INDEX_MAPPING)
    elif es_type == ES_TYPE_DISKOVER:
        new_settings["settings"] = copy.deepcopy(ES_INDEX_SETTINGS_DISKOVER)
        new_settings["settings"]["number_of_shards"] = options.get(
            "number_of_shards", ES_INDEX_SETTINGS["number_of_shards"]
        )
        new_settings["settings"]["number_of_replicas"] = options.get(
            "number_of_replicas", ES_INDEX_SETTINGS["number_of_replicas"]
        )
        new_settings["mapping"] = copy.deepcopy(ES_INDEX_MAPPING_DISKOVER)
    else:
        raise Exception(
            "Unknown ElasticSearch type: {es_type}. Could not create index settings.".format(es_type=es_type)
        )
    return new_settings


def es_create_start_options(options={}):
    es_options = {}
    if options.get("es_type") == ES_TYPE_DISKOVER:
        storage_usage_stats = {}
        if misc.is_onefs_os():
            storage_usage_stats = misc.get_local_storage_usage_stats()
        else:
            # TODO: Add support for querying for usage statistics
            pass
        ps_scan_ver = options.get("ps_scan_ver_str", "ps_scan v0.0.0")
        root_path = options.get("scan_path", "/ifs")
        es_options = {
            "diskover": {
                "spaceinfo": {
                    "available": storage_usage_stats.get("ifs.bytes.avail", 0),
                    "available_percent": storage_usage_stats.get("ifs.percent.avail", 0),
                    "free": storage_usage_stats.get("ifs.bytes.free", 0),
                    "free_percent": storage_usage_stats.get("ifs.percent.free", 0),
                    "total": storage_usage_stats.get("ifs.bytes.total", 0),
                    "used": storage_usage_stats.get("ifs.bytes.used", 0),
                    "path": root_path,
                    "type": "spaceinfo",
                },
                "indexinfo_start": {
                    "path": root_path,
                    "start_at": time.strftime(DEFAULT_TIME_FORMAT_8601),
                    "diskover_ver": ps_scan_ver,
                    "type": "indexinfo",
                },
            },
        }
    return es_options


def es_create_stop_options(options={}, stats={}):
    es_options = {}
    if options.get("es_type") == ES_TYPE_PS_SCAN:
        es_options = {
            "ps_scan": {
                "stats": stats,
            },
        }
    if options.get("es_type") == ES_TYPE_DISKOVER:
        root_path = options.get("scan_path", "/ifs")
        es_options = {
            "diskover": {
                "indexinfo_stop": {
                    "crawl_time": stats.get("total_time", 0),
                    "dir_count": stats.get("dirs_processed", 0),
                    "end_at": time.strftime(DEFAULT_TIME_FORMAT_8601),
                    "file_count": stats.get("files_processed", 0),
                    "file_size": stats.get("file_size_total", 0),
                    "file_size_du": stats.get("file_size_physical_total", 0),
                    "path": root_path,
                    "type": "indexinfo",
                },
            },
        }
    return es_options


def es_data_sender(
    send_q,
    cmd_q,
    url,
    username,
    password,
    es_cmd_idx,
    poll_interval=scanit.DEFAULT_POLL_INTERVAL,
    bulk_count=50,
):
    # TODO: Change the bulk_count and max_wait time to variables
    es_client = es_lite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    bulk_data = []
    max_wait_time = 30
    start_time = time.time()

    while True:
        flush = False
        flush_all = False
        cmd = None
        # Read commands from command queue.
        # If the command is exit we check if there is a flush argument and set the flush flag
        try:
            cmd_item = cmd_q.get(block=False)
            cmd = cmd_item[0]
            if cmd == CMD_EXIT:
                flush_all = (cmd_item[1] or {}).get("flush", False)
        except queue.Empty:
            pass
        except Exception as e:
            LOG.exception("Unable to get command from the ES command queue")

        # Read the data queue
        # This loop should run quickly and rapidly accumulate data to bulk send
        try:
            data_item = send_q.get(block=True, timeout=poll_interval)
            data_cmd = data_item[0]
            if data_cmd & (CMD_SEND | CMD_SEND_DIR | CMD_SEND_DIR_UPDATE | CMD_SEND_STATS):
                bulk_data.append([data_cmd, data_item[1]])
            elif data_cmd == CMD_EXIT:
                cmd = CMD_EXIT
                flush_all = True
        except queue.Empty:
            # If there is no data in the data queue and there are no items in our accumulation list, reset the start
            # time so we always wait up to the max_wait_time seconds to accumulate data
            if not bulk_data:
                start_time = time.time()
        except Exception as e:
            LOG.exception("Unable to get data from the ES queue")

        # If we have enough items in the bulk_data array or after a certain period of time has elapsed, send all the
        # data to Elastic by setting the flush flag to True
        if (len(bulk_data) >= bulk_count) or ((time.time() - start_time) >= max_wait_time):
            flush = True
        if flush or flush_all:
            # We bunch up our sends so the maximum number of times we should need to loop through the send_q items is
            # the number of items in the queue. This prevents the loop from running forever in case of an error.
            for flush_count in range(send_q.qsize() + 1):
                try:
                    LOG.debug(
                        {
                            "msg": "ElasticSearch bulk data flush",
                            "entries": len(bulk_data),
                            "flush_all": flush_all,
                            "flush_count": flush_count,
                            "qsize": send_q.qsize(),
                            "tid": threading.current_thread().name,
                        }
                    )
                    es_data_flush(bulk_data, es_client, es_cmd_idx)
                except Exception as e:
                    LOG.exception("Elastic search send failed.")
                    break
                bulk_data[:] = []
                if not flush_all:
                    break
                try:
                    for i in range(bulk_count):
                        data_item = send_q.get(block=False)
                        data_cmd = data_item[0]
                        if data_cmd & (CMD_SEND | CMD_SEND_DIR | CMD_SEND_DIR_UPDATE | CMD_SEND_STATS):
                            bulk_data.append([data_cmd, data_item[1]])
                except queue.Empty:
                    flush = True
                    flush_all = False
            start_time = time.time()
        if cmd == CMD_EXIT:
            break


def es_data_flush(bulk_data, es_client, es_cmd_idx):
    for chunk in bulk_data:
        bulk_file = []
        bulk_dir = []
        bulk_state = []
        chunk_type = chunk[0]
        chunk_data = chunk[1]
        # Each chunk_data represents a number of items
        # For each subitem in the chunk_data, encode the data as JSON and save it to the bulk_file or bulk_dir lists
        for idx in range(len(chunk_data)):
            body_text = None
            try:
                body_text = json.dumps(chunk_data[idx])
            except UnicodeDecodeError as ude:
                LOG.info("JSON dumps encountered unicode decoding error. Trying latin-1 re-code to UTF-8")
                try:
                    temp_text = json.dumps(chunk_data[idx], encoding="latin-1")
                    body_text = temp_text.decode("latin-1").encode("utf-8", errors="backslashreplace")
                except Exception as e:
                    LOG.warn("latin-1 backup decode failed. Dropping data.")
                    LOG.debug("latin-1 exception: %s" % e)
                    LOG.debug("Work item dump:\n%s" % chunk_data[idx])
            except Exception as e:
                LOG.warn("JSON dumps encountered an exception converting to text")
                LOG.debug("Work item dump:\n%s" % chunk_data[idx])
            if body_text:
                bulk_op = "update"
                body_text = '{"doc":' + body_text + ',"doc_as_upsert":true}'
                if chunk_type == CMD_SEND:
                    bulk_list = bulk_file
                elif chunk_type == CMD_SEND_DIR:
                    bulk_list = bulk_dir
                elif chunk_type == CMD_SEND_STATS:
                    bulk_list = bulk_state
                elif chunk_type == CMD_SEND_DIR_UPDATE:
                    bulk_list = bulk_dir
                # The following variables have 2 possible key names based on it being a ps_scan or diskover type
                inode = chunk_data[idx].get("inode", chunk_data[idx].get("ino"))
                file_path = chunk_data[idx].get("file_path", chunk_data[idx].get("parent_path"))
                file_name = chunk_data[idx].get("file_name", chunk_data[idx].get("name"))
                # The full path of the file including the file name is hashed with CRC32 in order to provide a unique
                # _id field for ElasticSearch. This is required for files that have identical inode numbers
                full_path = "%s/%s" % (file_path, file_name)
                bulk_list.append(json.dumps({bulk_op: {"_id": "%s_%s" % (inode, zlib.crc32(full_path) & 0xFFFFFFFF)}}))
                bulk_list.append(body_text)
        # For each bulk list, take all the entries and join them together with a \n and send them to the ES into the
        # appropriate index
        for bulk_list in [bulk_file, bulk_dir, bulk_state]:
            if bulk_list is bulk_file:
                idx = es_cmd_idx[CMD_SEND]
            elif bulk_list is bulk_dir:
                idx = es_cmd_idx[CMD_SEND_DIR]
            else:
                idx = es_cmd_idx[CMD_SEND_STATS]
            if not bulk_list:
                continue
            bulk_str = "\n".join(bulk_list)
            resp = es_client.bulk(bulk_str, index_name=idx)
            if resp.get("error", False):
                LOG.error(resp["error"])
            if resp.get("errors", False):
                for item in resp.get("items"):
                    op_keys = item.keys()
                    if op_keys in ["index", "update"]:
                        if item[op_keys]["status"] < 200 or item[op_keys]["status"] > 299:
                            LOG.error(json.dumps(item[op_keys]["error"]))


def es_delete_index(es_client, es_cmd_idx):
    LOG.debug("Delete existing indices")
    index_names = []
    for key in es_cmd_idx.keys():
        index_names.append(es_cmd_idx[key])
        index_names = list(set(index_names))
    for idx in index_names:
        resp = es_client.delete_index(idx)
        if resp.get("status", 200) not in [200, 404]:
            LOG.error(json.dumps(resp.get("error", {})))
        LOG.debug("Delete index (%s) response: %s" % (idx, resp))


def es_init_index(es_client, es_cmd_idx, settings={}, options={}):
    index_names = []
    for key in es_cmd_idx.keys():
        index_names.append(es_cmd_idx[key])
        index_names = list(set(index_names))
    for idx in index_names:
        LOG.debug("Creating new index: {idx}".format(idx=idx))
        resp = es_client.create_index(
            idx,
            mapping=settings.get("mapping", {}),
            settings=settings.get("settings", {}),
        )
        LOG.debug("Create index response: %s" % resp)
        if resp.get("status", 200) not in [200]:
            if resp["error"].get("type") == "resource_already_exists_exception":
                LOG.debug("Index {idx} already exists".format(idx=idx))
            else:
                LOG.error(json.dumps(resp.get("error", {})))
                raise Exception("Unable to create index")


def es_query_match(es_client, es_cmd_idx, query_match=None, query_ops=None):
    query_ops = query_ops or {}
    if not query_match:
        return
    query = {
        "query": {
            "match": query_match,
        },
    }
    op_keys = query_ops.keys()
    for field in ["_source", "fields"]:
        if field in op_keys:
            query[field] = query_ops[field]
    LOG.critical({"msg": "Sending ES match query", "query": query})
    index_names = []
    for key in es_cmd_idx.keys():
        index_names.append(es_cmd_idx[key])
        index_names = list(set(index_names))
    for idx in index_names:
        resp = es_client.search(body_str=query, index_name=idx)
        LOG.critical("DEBUG: RESPONSE DUMP: %s" % json.dumps(resp, sort_keys=True))
        # if resp.get("status", 200) not in [200, 404]:
        #    LOG.error(json.dumps(resp.get("error", {})))
        # LOG.debug("Query [match] index (%s) response: %s" % (idx, resp))


def es_start_processing(es_client, es_cmd_idx, start_options={}, options={}):
    es_type = options.get("es_type")
    index_names = []
    for key in es_cmd_idx.keys():
        index_names.append(es_cmd_idx[key])
        index_names = list(set(index_names))
    if es_type == ES_TYPE_PS_SCAN:
        # When using Elasticsearch, set the index configuration to speed up bulk updates
        LOG.debug("Updating index settings")
        body = ES_REFERSH_INTERVAL % start_options.get("bulk_refresh_interval", DEFAULT_ES_BULK_REFRESH_INTERVAL)
        for idx in index_names:
            resp = es_client.update_index_settings(body_str=body, index_name=idx)
            if resp.get("status", 200) not in [200]:
                LOG.error(resp)
    elif es_type == ES_TYPE_DISKOVER:
        diskover_options = start_options.get("diskover", {})
        index_name = es_cmd_idx[CMD_SEND]
        LOG.debug("Updating index settings")
        body = ES_REFERSH_INTERVAL % start_options.get("bulk_refresh_interval", DEFAULT_ES_BULK_REFRESH_INTERVAL)
        resp = es_client.update_index_settings(body_str=body, index_name=index_name)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
        body = diskover_options.get("spaceinfo")
        if body:
            LOG.debug("Creating Diskover spaceinfo document.")
            resp = es_client.post_document(body, index_name=index_name)
            if resp.get("status", 200) not in [200]:
                LOG.error(resp)
        body = diskover_options.get("indexinfo_start")
        if body:
            LOG.debug("Creating Diskover start indexinfo document.")
            resp = es_client.post_document(body, index_name=index_name)
            if resp.get("status", 200) not in [200]:
                LOG.error(resp)
    else:
        raise Exception("Unknown ElasticSearch type: {es_type}. Could not start processing.".format(es_type=es_type))


def es_stop_processing(es_client, es_cmd_idx, stop_options={}, options={}):
    es_type = options.get("es_type")
    index_names = []
    for key in es_cmd_idx.keys():
        index_names.append(es_cmd_idx[key])
        index_names = list(set(index_names))
    if es_type not in (ES_TYPE_DISKOVER, ES_TYPE_PS_SCAN):
        raise Exception("Unknown ElasticSearch type: {es_type}. Could not stop processing.".format(es_type=es_type))
    if es_type == ES_TYPE_DISKOVER:
        diskover_options = stop_options.get("diskover", {})
        body = diskover_options.get("indexinfo_stop")
        if body:
            LOG.debug("Creating Diskover stop indexinfo document.")
            resp = es_client.post_document(body, index_name=index_names[0])
            if resp.get("status", 200) not in [200]:
                LOG.error(resp)
    # When using Elasticsearch, reset the index configuration after bulk updates and flush the index
    LOG.debug("Reset index settings, flush, and force merge")
    body = ES_REFERSH_INTERVAL % stop_options.get("standard_refresh_interval", DEFAULT_ES_STANDARD_REFRESH_INTERVAL)
    for idx in index_names:
        resp = es_client.update_index_settings(body_str=body, index_name=idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
        resp = es_client.flush(idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
        resp = es_client.forcemerge(idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
