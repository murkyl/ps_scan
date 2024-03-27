#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Misc helper functoons
"""
# fmt: off
__title__         = "misc"
__version__       = "1.0.0"
__date__          = "10 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "ace_list_to_str_list",
    "acl_group_to_str",
    "acl_user_to_str",
    "chunk_list",
    "get_directory_listing",
    "get_local_internal_addr",
    "get_local_node_number",
    "get_local_storage_usage_stats",
    "get_nodepool_translation",
    "get_path_from_urlencoded",
    "humanize_number",
    "humanize_seconds",
    "is_onefs_os",
    "merge_process_stats",
    "parse_arg_bool",
    "parse_arg_int",
    "parse_bool",
    "parse_float",
    "parse_int",
    "parse_node_list",
    "parse_regex_file",
    "read_es_options_file",
    "set_resource_limits",
    "split_numeric_range_list",
    "sysctl",
    "sysctl_raw",
]
# fmt: on
import copy
import errno
import json
import logging
import os
import platform
import re
import subprocess

try:
    import resource
except:
    pass
try:
    import urllib.parse

    urlencode = urllib.parse.urlencode
    urlparse = urllib.parse.urlparse
    urlquote = urllib.parse.quote
    urlunquote = urllib.parse.unquote
    urlunsplit = urllib.parse.urlunsplit
except:
    import urllib
    import urlparse as py27_urlparse

    urlencode = urllib.urlencode
    urlparse = py27_urlparse.urlparse
    urlquote = urllib.quote
    urlunquote = urllib.unquote
    urlunsplit = py27_urlparse.urlunsplit

from .constants import *
import libs.papi_lite as papi_lite

try:
    import isi.fs.diskpool as dp
except:
    pass
try:
    dir(os.scandir)
    USE_SCANDIR = 1
except:
    USE_SCANDIR = 0

ELASTIC_VALID_INDEX_RE_STR = r'^[^-_+\.][^A-Z\\\/\*\?"<>| ,#:]+$'
ELASTIC_VALID_INDEX_LENGTH = 255
LOG = logging.getLogger(__name__)
URI_STATISTICS_KEY = "/statistics/current"


def ace_list_to_str_list(ace_list):
    """Convert a list of ACEs into a list of strings representing the ACEs in the form:
    type: entity[user|group] permission_type[allow|deny] permissions flags

    Parameters
    ----------
    ace_list: list of dict - List of ACE dictionaries

    Returns
    ----------
    list of str - A list of ACE strings.

    An empty list is returned on invalid input.
    """
    ace_strs = []
    if not ace_list:
        return ace_strs
    for ace in ace_list:
        perm_str = "{etype}:{entity} {ptype} {perms} {flags}".format(
            etype=ace["entity_type"],
            entity=ace["entity"],
            ptype=ace["perm_type_str"],
            perms=",".join(ace["perms_list"]),
            flags=",".join(ace["flags_list"]),
        )
        ace_strs.append(perm_str)
    return ace_strs


def acl_group_to_str(acl):
    """Convert an ACL into a string representing the group ownership in the form:
    type[gid|sid]: entity

    Parameters
    ----------
    acl: dict - Dictionary containing an ACL

    Returns
    ----------
    str - A string for the group ownership.

    An empty string is returned on invalid input.
    """
    if not acl:
        return ""
    return "{gtype}:{group}".format(gtype=acl.get("group_type"), group=acl.get("group"))


def acl_user_to_str(acl):
    """Convert an ACL into a string representing the user ownership in the form:
    type[uid|sid]: entity

    Parameters
    ----------
    acl: dict - Dictionary containing an ACL

    Returns
    ----------
    str - A string for the user ownership.

    An empty string is returned on invalid input.
    """
    if not acl:
        return ""
    return "{utype}:{user}".format(utype=acl.get("user_type"), user=acl.get("user"))


def chunk_list(list_data, chunks):
    chunked_list = [[] for x in range(chunks)]
    chunk_sizes = [(len(list_data) // chunks) + (1 * (i < (len(list_data) % chunks))) for i in range(chunks)]
    index = 0
    for i in range(chunks):
        chunked_list[i] = list_data[index : index + chunk_sizes[i]]
        index += chunk_sizes[i]
    return chunked_list


def get_directory_listing(path):
    try:
        if USE_SCANDIR:
            dir_file_list = []
            for entry in os.scandir(path):
                dir_file_list.append(entry.name)
        else:
            dir_file_list = os.listdir(path)
    except IOError as ioe:
        dir_file_list = None
        if ioe.errno == errno.EACCES:  # 13: No access
            LOG.debug({"msg": "Directory permission error", "path": path, "error": str(ioe)})
        elif ioe.errno == 20:  # 20: Not a directory
            LOG.info({"msg": "Unable to list path that is not a directory", "path": path})
        else:
            LOG.debug({"msg": "Unknown error", "path": path, "error": str(ioe)})
    except PermissionError as pe:
        dir_file_list = None
        LOG.debug({"msg": "Directory permission error", "path": path, "error": str(pe)})
    except Exception as e:
        dir_file_list = None
        LOG.debug({"msg": "Unknown error", "path": path, "error": str(e)})
    return dir_file_list


def get_local_internal_addr():
    """Return the preferred system internal backend network IP address.
    Works only on a OneFS cluster.

    Parameters
    ----------
    None

    Returns
    ----------
    str|None - String with the preferred IP address in dotted decimal notation. e.g. 1.2.3.4
        If the platform the script is running on is not OneFS
    """
    if not is_onefs_os():
        return None
    subproc = subprocess.Popen(
        ["isi_nodes", "-L", '"%{internal}"'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = subproc.communicate()
    addr = (stdout.decode("UTF-8")).strip().replace('"', "")
    return addr


def get_local_node_number():
    """Return the preferred system internal backend network IP address.
    Works only on a OneFS cluster.

    Parameters
    ----------
    None

    Returns
    ----------
    int|None - The logical node number of the node running the script
        If the platform the script is running on is not OneFS, return None
    """
    if not is_onefs_os():
        return None
    subproc = subprocess.Popen(
        ["isi_nodes", "-L", '"%{lnn}"'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout, stderr = subproc.communicate()
    lnn = (stdout.decode("UTF-8")).strip().replace('"', "")
    return lnn


def get_local_storage_usage_stats():
    stats = {}
    stats_keys = [
        "ifs.bytes.avail",
        "ifs.percent.avail",
        "ifs.bytes.free",
        "ifs.percent.free",
        "ifs.bytes.total",
        "ifs.bytes.used",
    ]
    papi = papi_lite.papi_lite()
    data = papi.rest_call(
        URI_STATISTICS_KEY,
        "GET",
        query_args={"keys": ",".join(stats_keys)},
    )
    if data[0] != 200:
        raise Exception("Error in PAPI request to {url}:\n{err}".format(err=str(data), url=URI_STATISTICS_KEY))
    for item in data[2].get("stats"):
        stats[item["key"]] = item["value"]
    return stats


def get_nodepool_translation():
    """Returns a dictionary that maps between the disk pool DB ID number and the name of the pool

    When looking at a file's OneFS inode metadata, the node pool that the file resides in is represented by a number
    in the disk pool database. This function will translate that number into a human readable name when a translation
    is available.

    Parameters
    ----------
    None

    Returns
    ----------
    dict - A dictionary with the disk pool ID to name mapping
        {
          <disk_pool_id_1>: str,
          <disk_pool_id_2>: str,
          <disk_pool_id_...>: str,
        }
    """
    nodepool_translation = {}
    if is_onefs_os():
        try:
            dpdb = dp.DiskPoolDB()
            groups = dpdb.get_groups()
            for g in groups:
                children = g.get_children()
                for child in children:
                    nodepool_translation[int(child.entryid)] = g.name
        except Exception as e:
            LOG.exception("Unable to get the ID to name translation for node pools")
    else:
        LOG.info({"msg": "Cannot get nodepool translation table. Not running on a OneFS system"})
    return nodepool_translation


def get_path_from_urlencoded(urlencoded_path):
    decoded_path = urlunquote(urlencoded_path)
    if decoded_path.endswith("/"):
        decoded_path = decoded_path[0:-1]
    if not decoded_path.startswith("/"):
        decoded_path = "/" + decoded_path
    if not decoded_path.startswith("/ifs"):
        decoded_path = "/ifs" + decoded_path
    root = os.path.dirname(decoded_path)
    file = os.path.basename(decoded_path)
    return root, file, decoded_path


def humanize_number(num, suffix="B", base=10, truncate=True):
    num = num if num else 0
    factor = 1024.0 if base == 2 else 1000.0
    bin_mark = ""
    if num == 0:
        return "0 %s" % suffix
    for unit in ["", "K", "M", "G", "T", "P", "E", "Z", "Y"]:
        if abs(num) < factor:
            break
        num /= factor
    if unit != "" and base == 2:
        bin_mark = "i"
    return "%.1f %s%s%s" % (num, unit, bin_mark, suffix)


def humanize_seconds(sec):
    units = [[31536000, "year"], [86400, "day"], [3600, "hour"], [60, "minute"], [1, "second"]]
    if sec == 0:
        return "0 seconds"
    elif not sec:
        return ""
    time_str = ""
    for unit in units:
        if abs(sec) >= unit[0]:
            whole = sec // unit[0]
            time_str += "%d %s%s " % (whole, unit[1], "s" * (whole > 1) or "" * (whole <= 1))
            sec -= whole * unit[0]
    time_str = time_str.rstrip()
    return time_str


def is_onefs_os():
    return "OneFS" in platform.system()


def merge_process_stats(process_states):
    temp_stats = None
    for state in process_states.values():
        if not state["stats"]:
            # No stats for this process yet
            continue
        if temp_stats is None and state["stats"]:
            temp_stats = copy.deepcopy(state["stats"])
            continue
        for key in temp_stats.keys():
            if key in state["stats"]:
                if key == "custom":
                    continue
                temp_stats[key] += state["stats"][key]
    return temp_stats


def parse_arg_bool(arg, field, default):
    val = arg.get(field, default)
    if isinstance(val, bool):
        return val
    val = val.lower()
    if val in ["false", "0"]:
        return False
    return True


def parse_arg_int(arg, field, default, minimum=None, maximum=None):
    val = int(arg.get(field, default))
    if minimum and val < minimum:
        val = minimum
    if maximum and val > maximum:
        val = maximum
    return val


def parse_bool(text):
    if not text:
        return False
    ltext = text.lower()
    if ltext in ["none"]:
        return None
    if ltext in ["true", "t", "1"]:
        return True
    return False


def parse_float(text, minimum=None, maximum=None):
    if not text:
        return 0
    if text.lower() in ["none"]:
        return None
    val = float(text)
    if minimum and val < minimum:
        return minimum
    if maximum and val > maximum:
        return maximum
    return val


def parse_int(text, minimum=None, maximum=None):
    if not text:
        return 0
    if text.lower() in ["none"]:
        return None
    val = int(text)
    if minimum and val < minimum:
        return minimum
    if maximum and val > maximum:
        return maximum
    return val


def parse_node_list(node_str, min_node_list=[]):
    lnn_list = split_numeric_range_list(node_str)
    if not lnn_list:
        lnn_list = min_node_list
    node_list = [{"endpoint": str(lnn), "type": "onefs"} for lnn in lnn_list]
    return node_list


def parse_regex_file(filename):
    regex_array = []
    with open(filename, "r") as f:
        lines = f.readlines()
        for line in lines:
            if line:
                regex_array.append(re.compile(line.strip()))
    return regex_array


def read_es_options_file(filename):
    es_creds = {}
    try:
        with open(filename, "r") as option_file:
            es_creds = json.load(option_file)
    except Exception as e:
        LOG.warn("Using a non-JSON ES options file is deprecated and support may be removed in the future.")
        try:
            with open(filename) as option_file:
                lines = option_file.readlines()
                es_creds["user"] = lines[0].strip()
                es_creds["password"] = lines[1].strip()
                if len(lines) > 2:
                    es_creds["index"] = lines[2].strip()
                if len(lines) > 3:
                    es_creds["url"] = lines[3].strip()
                # Optional 5th parameter, the type of ES index. Normally ps_scan or diskover
                if len(lines) > 4:
                    es_type = lines[4].strip()
                    if es_type:
                        es_creds["type"] = es_type
        except Exception as e:
            raise Exception("Unable to open or read the credentials file: {filename}".format(filename=filename))
    # Validate options
    if "user" not in es_creds:
        es_creds["user"] = ""
    if "password" not in es_creds:
        es_creds["password"] = ""
    if "type" not in es_creds:
        es_creds["type"] = ES_TYPE_PS_SCAN
    if "index" not in es_creds:
        raise Exception("An ElasticSearch index name is required")
    # Validate the ES URL is well formed
    if not es_creds.get("url") or not es_creds["url"].startswith("http"):
        raise Exception("Invalid ElasticSearch URL: {url}".format(url=es_creds.get("url")))
    parsed_url = urlparse(es_creds["url"])
    es_creds["url"] = urlunsplit([parsed_url.scheme, parsed_url.netloc, "", "", ""])
    # Validate the ES index name meets allowed characters
    valid_index = re.match(ELASTIC_VALID_INDEX_RE_STR, es_creds["index"])
    if not valid_index:
        raise Exception("ElasticSearch index name must match this regex: {exp}".format(exp=ELASTIC_VALID_INDEX_RE_STR))
    # Validate the index name is <= ELASTIC_VALID_INDEX_LENGTH bytes
    byte_str = es_creds["index"].encode("UTF-8")
    if len(byte_str) > ELASTIC_VALID_INDEX_LENGTH:
        raise Exception("ElasticSearch index name exceeds {size} bytes".format(size=ELASTIC_VALID_INDEX_LENGTH))
    if es_creds["type"] not in [ES_TYPE_PS_SCAN, ES_TYPE_DISKOVER]:
        raise Exception("Invalid type for ElasticSearch index format: {type}".format(type=es_creds["type"]))
    return es_creds


def set_resource_limits(min_memory=DEFAULT_ULIMIT_MEMORY, force=False):
    old_limit = None
    new_limit = None
    if not is_onefs_os() and not force:
        return (None, None)
    try:
        old_limit = resource.getrlimit(resource.RLIMIT_VMEM)
    except Exception as e:
        pass
    try:
        physmem = int(sysctl("hw.physmem"))
    except Exception as e:
        physmem = 0
    try:
        if physmem >= min_memory or force:
            if old_limit is None or min_memory > old_limit[1]:
                resource.setrlimit(resource.RLIMIT_VMEM, (min_memory, min_memory))
                new_limit = min_memory
            else:
                new_limit = old_limit
    except Exception as e:
        return (None, None)
    return (old_limit, new_limit)


def split_numeric_range_list(range_list):
    if not range_list:
        return []
    groups = [ri.split("-") for ri in range_list.split(",")]
    flat_list = sorted(sum([list(range(int(x[0]), int(x[-1]) + 1)) for x in groups], []))
    return flat_list


def sysctl(name, newval=None):
    """Set or get the value of a sysctl and return the results as a UTF-8 string

    Parameters
    ----------
    name: str - Sysctl name in full dot notation. e.g. efs.bam.ec.mode
    newval: str|None - String value to set the sysctl. Use None to read a value

    Returns
    ----------
    str - Result of the sysctl command
    """
    sysctl_out = sysctl_raw(name, newval)
    data = sysctl_out.decode("UTF-8")
    kv = data.split(": ", 1)
    return kv[1]


def sysctl_raw(name, newval=None):
    cmd_line = ["sysctl", name]
    if newval:
        cmd_line.append("=")
        cmd_line.append(newval)
    proc = subprocess.Popen(cmd_line, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = proc.communicate()
    if stderr:
        raise Exception(stderr.decode("UTF-8"))
    if not stdout.decode("UTF-8").startswith(name):
        raise Exception(stdout.decode("UTF-8"))
    return stdout
