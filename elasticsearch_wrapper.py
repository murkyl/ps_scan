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
    "es_delete_index",
    "es_init_index",
    "es_start_processing",
    "es_stop_processing",
]
# fmt: on
import json
import logging
import queue
import socket

import elasticsearch_lite
import scanit
from helpers.constants import *

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
                    bulk_data.append(json.dumps({"index": {"_id": work_items[i]["inode"]}}))
                    bulk_data.append(json.dumps(work_items[i]))
                    work_items[i] = None
                bulk_str = "\n".join(bulk_data)
                resp = es_client.bulk(bulk_str, index_name=file_idx if cmd == CMD_SEND else dir_idx)
                if resp.get("error", False):
                    LOG.error(resp["error"])
                if resp.get("errors", False):
                    for item in resp.get("items"):
                        if item["index"]["status"] != 200:
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
            break


def es_create_connection(url, username, password, index):
    if index[-1] == "_":
        index = index[0:-1]
    es_client = elasticsearch_lite.ElasticsearchLite()
    es_client.username = username
    es_client.password = password
    es_client.endpoint = url
    return [es_client, [index + x for x in ["_file", "_dir"]]]


def es_delete_index(es_client):
    LOG.debug("Delete existing indices")
    for idx in es_client[1]:
        resp = es_client[0].delete_index(idx)
        if resp.get("status", 200) not in [200, 404]:
            LOG.error(json.dumps(resp.get("error", {})))
        LOG.debug("Delete index (%s) response: %s" % (idx, resp))


def es_init_index(es_client, index):
    LOG.debug("Creating new indices with mapping: {idx}_file and {idx}_dir".format(idx=index))
    for idx in es_client[1]:
        resp = es_client[0].create_index(idx, mapping=PS_SCAN_MAPPING)
        LOG.debug("Create index response: %s" % resp)
        if resp.get("status", 200) not in [200]:
            if resp["error"]["type"] == "resource_already_exists_exception":
                LOG.debug("Index {idx} already exists".format(idx=idx))
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
