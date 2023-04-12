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
    "is_onefs_os",
    "merge_process_stats",
]
# fmt: on
import copy
import platform


def ace_list_to_str_list(ace_list):
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
    if not acl:
        return ""
    return "{gtype}:{group}".format(gtype=acl.get("group_type"), group=acl.get("group"))


def acl_user_to_str(acl):
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


def is_onefs_os():
    return "OneFS" in platform.system()


def merge_process_stats(process_states):
    temp_stats = None
    for state in process_states:
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
