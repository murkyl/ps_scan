#!/usr/bin/env python
# coding: utf-8
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
    "get_local_internal_addr",
    "get_local_node_number",
    "get_local_storage_usage_stats",
    "is_onefs_os",
    "merge_process_stats",
    "parse_node_list",
    "read_es_options_file",
    "set_resource_limits",
    "split_numeric_range_list",
    "sysctl",
    "sysctl_raw",
]
# fmt: on
import copy
import platform

try:
    import resource
except:
    pass
import subprocess

from .constants import *
import libs.papi_lite as papi_lite


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
        If the platform the script is running on is not OneFS
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


def parse_node_list(node_str, min_node_list=[]):
    lnn_list = split_numeric_range_list(node_str)
    if not lnn_list:
        lnn_list = min_node_list
    node_list = [{"endpoint": str(lnn), "type": "onefs"} for lnn in lnn_list]
    return node_list


def read_es_options_file(filename):
    es_creds = {}
    try:
        with open(filename) as f:
            lines = f.readlines()
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
        return None
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
