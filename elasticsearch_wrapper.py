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
    "es_create_settings",
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
import socket

import elasticsearch_lite
import scanit
from helpers.constants import *

ES_INDEX_MAPPING = {
    "properties": {
        # ========== Timestamps ==========
        # Time when this entry was scanned
        "_scan_time": {"type": "long"},
        # Last access time of the file both in fractional seconds and YYYY-mm-DD format
        "atime": {"type": "long"},
        "atime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Birth/creation time of the file both in fractional seconds and YYYY-mm-DD format
        "btime": {"type": "long"},
        "btime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Last metadata change time of the file both in fractional seconds and YYYY-mm-DD format
        "ctime": {"type": "long"},
        "ctime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Last data modified time of the file both in fractional seconds and YYYY-mm-DD format
        "mtime": {"type": "long"},
        "mtime_date": {"type": "date", "format": "yyyy-MM-dd"},
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
ES_INDEX_SETTINGS = {
    "number_of_shards": 1,
    "max_regex_length": 4096,
}
ES_REFERSH_INTERVAL = """{"index":{"refresh_interval":"%s"}}"""
LOG = logging.getLogger(__name__)


def es_data_sender(send_q, cmd_q, url, username, password, index_name, poll_interval=scanit.DEFAULT_POLL_INTERVAL):
    es_client = elasticsearch_lite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    file_idx = index_name + "_file"
    dir_idx = index_name + "_dir"

    while True:
        # Process our command queue
        try:
            cmd_item = cmd_q.get(block=False)
            cmd = cmd_item[0]
            if cmd == CMD_EXIT:
                break
        except queue.Empty:
            pass
        except Exception as e:
            LOG.exception(e)
        # Process send queue
        try:
            cmd_item = send_q.get(block=True, timeout=poll_interval)
            cmd = cmd_item[0]
            if cmd == CMD_EXIT:
                break
            elif cmd & (CMD_SEND | CMD_SEND_DIR):
                # TODO: Optimize this section by using a byte buffer and writing directly into the buffer?
                bulk_data = []
                work_items = cmd_item[1]
                for i in range(len(work_items)):
                    try:
                        body_text = json.dumps(work_items[i])
                    except UnicodeDecodeError as ude:
                        LOG.info("JSON dumps encountered unicode decoding error. Trying latin-1 re-code to UTF-8")
                        try:
                            temp_text = json.dumps(work_items[i], encoding="latin-1")
                            body_text = body_text.decode("latin-1").encode("utf-8", errors="backslashreplace")
                        except Exception as e:
                            LOG.info("latin-1 backup decode failed. Dropping data.")
                            LOG.debug("latin-1 exception: %s" % e)
                            LOG.debug("Work item dump:\n%s" % work_items[i])
                            body_text = None
                    except Exception as e:
                        LOG.info("JSON dumps encountered an exception converting to text")
                        LOG.debug("Work item dump:\n%s" % work_items[i])
                    if body_text:
                        bulk_data.append(json.dumps({"index": {"_id": work_items[i]["inode"]}}))
                        bulk_data.append(body_text)
                    work_items[i] = None
                if bulk_data:
                    bulk_str = "\n".join(bulk_data)
                    resp = es_client.bulk(bulk_str, index_name=file_idx if cmd == CMD_SEND else dir_idx)
                    if resp.get("error", False):
                        LOG.error(resp["error"])
                    if resp.get("errors", False):
                        for item in resp.get("items"):
                            if item["index"]["status"] < 200 or item["index"]["status"] > 299:
                                LOG.error(json.dumps(item["index"]["error"]))
                del bulk_str
                del bulk_data
                del cmd_item
        except queue.Empty:
            pass
        except socket.gaierror as gaie:
            LOG.critical("Elasticsearch URL is invalid")
            break
        except Exception as e:
            LOG.exception(e)
            LOG.debug("Work items triggering exception:\n%s" % work_items[i])


def es_create_connection(url, username, password, index):
    if index[-1] == "_":
        index = index[0:-1]
    es_client = elasticsearch_lite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    return [es_client, [index + x for x in ["_file", "_dir"]], "%s_state" % index]


def es_create_settings(options=None):
    if not options:
        return None
    new_settings = copy.deepcopy(ES_INDEX_SETTINGS)
    new_settings["number_of_shards"] = options.es_shards
    return new_settings


def es_delete_index(es_client):
    LOG.debug("Delete existing indices")
    for idx in es_client[1]:
        resp = es_client[0].delete_index(idx)
        if resp.get("status", 200) not in [200, 404]:
            LOG.error(json.dumps(resp.get("error", {})))
        LOG.debug("Delete index (%s) response: %s" % (idx, resp))
    resp = es_client[0].delete_index(es_client[2])
    if resp.get("status", 200) not in [200, 404]:
        LOG.error(json.dumps(resp.get("error", {})))
    LOG.debug("Delete index (%s) response: %s" % (es_client[2], resp))


def es_init_index(es_client, index, settings=None):
    LOG.debug("Creating new indices with mapping: {idx}_file and {idx}_dir".format(idx=index))
    for idx in es_client[1]:
        resp = es_client[0].create_index(idx, mapping=ES_INDEX_MAPPING, settings=settings)
        LOG.debug("Create index response: %s" % resp)
        if resp.get("status", 200) not in [200]:
            if resp["error"]["type"] == "resource_already_exists_exception":
                LOG.debug("Index {idx} already exists".format(idx=idx))
            else:
                LOG.error(json.dumps(resp.get("error", {})))
    resp = es_client[0].create_index(es_client[2], settings=settings)
    LOG.debug("Create index response: %s" % resp)
    if resp.get("status", 200) not in [200]:
        if resp["error"]["type"] == "resource_already_exists_exception":
            LOG.debug("Index {idx} already exists".format(idx=es_client[2]))
        else:
            LOG.error(json.dumps(resp.get("error", {})))


def es_start_processing(es_client, options):
    # When using Elasticsearch, set the index configuration to speed up bulk updates
    LOG.debug("Updating index settings")
    body = ES_REFERSH_INTERVAL % options.es_bulk_refresh
    for idx in es_client[1]:
        resp = es_client[0].update_index_settings(body_str=body, index_name=idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)


def es_stop_processing(es_client, options):
    # When using Elasticsearch, reset the index configuration after bulk updates and flush the index
    LOG.debug("Reset index settings, flush, and force merge")
    body = ES_REFERSH_INTERVAL % "1s"
    for idx in es_client[1]:
        resp = es_client[0].update_index_settings(body_str=body, index_name=idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
        resp = es_client[0].flush(idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
        resp = es_client[0].forcemerge(idx)
        if resp.get("status", 200) not in [200]:
            LOG.error(resp)
