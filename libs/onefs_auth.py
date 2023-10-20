#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "onefs_auth"
__version__       = "1.0.0"
__date__          = "25 September 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "GetPrincipalName",
    "translate_user_group_perms",
]
# fmt: on
import logging
import os
import re
import threading
import time

import isi.fs.attr as iattr
import libs.papi_lite as papi_lite

GET_AUTH_TYPE_AUTO = 0
GET_AUTH_TYPE_GROUP = 1
GET_AUTH_TYPE_USER = 2
LOG = logging.getLogger(__name__)
MAX_CACHE_SIZE = 16384
MAX_CACHE_TS_BLOCK_TIME = 60
MAX_ZONE_CACHE_SIZE = 1024
URI_ACCESS_ZONES = "/zones"
URI_AUTH_GROUP = "/auth/groups"
URI_AUTH_USER = "/auth/users"
WELLKNOWN_SID_TABLE = {
    "SID:S-1-1-0": {"ts": 0, "name": "Everyone"},
    "SID:S-1-2-0": {"ts": 0, "name": "Local"},
    "SID:S-1-3-0": {"ts": 0, "name": "Creator Owner"},
    "SID:S-1-3-1": {"ts": 0, "name": "Creator Group"},
}


# TODO:
# Need some method to keep track of cache size and to clear out all the older entries based on the "ts" field
#
class GetPrincipalName:
    def __init__(self):
        self.lock = threading.Lock()
        self.papi_handle = papi_lite.papi_lite()
        # Stores all possible path depth lengths of all access zones
        self.zone_path_depths = []
        # A map of the path depth to a map of the actual path to the zone name
        # e.g.: {3: {"/ifs/data/zone1": "az_name1"}, 1: {"/ifs": "System"}}
        self.path_depth_zone_name_map = {}
        # Maps a zone name to a map of a principal's ID to a principal
        # e.g.: {"System": {"uid:0": "root"}}
        self.zone_auth_cache = {}
        self.zone_auth_cache_max = MAX_CACHE_SIZE + 1
        self.zone_auth_cache_size = 0
        self.zone_auth_cache_ts = 0
        self.zone_auth_cache_ts_block = 1
        self.zone_auth_cache_ts_block_last = 0
        self.zone_auth_cache_ts_block_time = MAX_CACHE_TS_BLOCK_TIME
        self.zone_auth_cache_ts_lock = threading.Lock()
        # Simple dictionary to cache a path to a set of access zone names
        self.path_zone_cache = {}
        self.path_zone_cache_len = MAX_ZONE_CACHE_SIZE
        self.path_zone_cache_fifo = []
        self._init_access_zone_list()
        self.principal_check = re.compile(r"(GID|SID|UID)")

    def _get_cache_ts(self):
        if not self.zone_auth_cache_ts:
            self.zone_auth_cache_ts = time.time()
        if time.time() - self.zone_auth_cache_ts > self.zone_auth_cache_ts_block_time:
            self.zone_auth_cache_ts_lock.acquire()
            self.zone_auth_cache_ts_block += 1
            self.zone_auth_cache_ts_lock.release()
            self.zone_auth_cache_ts = time.time()
        return self.zone_auth_cache_ts_block

    def _init_access_zone_list(self):
        data = self.papi_handle.rest_call(URI_ACCESS_ZONES, "GET")
        if data[0] != 200:
            raise ({"msg": "Error occurred while trying to get cluster access zone list", "err": data})
        for zone in data[2]["zones"]:
            zname = str(zone["name"])
            path_parts = zone["path"].split("/")
            path_depth = len(path_parts) - 1
            if path_depth not in self.path_depth_zone_name_map:
                self.path_depth_zone_name_map[path_depth] = {}
            self.path_depth_zone_name_map[path_depth][zone["path"]] = zname
            self.zone_auth_cache[zname] = dict(WELLKNOWN_SID_TABLE)
        self.zone_path_depths = sorted(self.path_depth_zone_name_map.keys(), reverse=True)

    def expire_cache(self):
        # Process expiring old cache entries
        if self.zone_auth_cache_size < self.zone_auth_cache_max:
            return
        try:
            self.lock.acquire()
            self.zone_auth_cache_ts_lock.acquire()
            if self.zone_auth_cache_ts_block_last >= self.zone_auth_cache_ts_block:
                return
            self.zone_auth_cache_ts_block_last += 1
            for zone in self.zone_auth_cache.values():
                for principal_key in list(zone.keys()):
                    if zone[principal_key]["ts"] and zone[principal_key]["ts"] <= self.zone_auth_cache_ts_block_last:
                        del zone[principal_key]
                        self.zone_auth_cache_size -= 1
        finally:
            self.zone_auth_cache_ts_lock.release()
            self.lock.release()

    def get_group_name(self, principal, path, strict=False, zone_list=None):
        principal = str(principal).upper()
        if "GID" not in principal and "SID" not in principal:
            principal = "GID:" + principal
        return self.get_principal_name(principal, path, GET_AUTH_TYPE_GROUP, strict=strict, zone_list=zone_list)

    def get_principal_name(self, principal, path, principal_type=GET_AUTH_TYPE_AUTO, strict=False, zone_list=None):
        principal = str(principal).upper()
        if principal_type not in [GET_AUTH_TYPE_GROUP, GET_AUTH_TYPE_USER]:
            # Try and figure out the principal name by the principal string:
            principal_type = GET_AUTH_TYPE_GROUP if "GID" in principal else GET_AUTH_TYPE_USER
        if principal_type == GET_AUTH_TYPE_USER:
            base_type = "users"
            base_uri = URI_AUTH_USER
        elif principal_type == GET_AUTH_TYPE_GROUP:
            base_type = "groups"
            base_uri = URI_AUTH_GROUP
        else:
            raise Exception({"msg": "Unknown principal type", "type": principal_type})
        
        if not zone_list:
            zone_list = self.get_zones_for_path(path)
        
        add_to_cache = []
        principal_entry = None
        for zone_name in zone_list:
            zone = self.zone_auth_cache.get(zone_name)
            if not zone:
                raise Exception({"msg": "Invalid zone name while getting principal name", "zone": zone_name})
            principal_entry = zone.get(principal)
            if principal_entry:
                break
            add_to_cache.append(zone_name)
            principal_data = self.papi_handle.rest_call(
                base_uri + "/" + principal, "GET", query_args={"query_member_of": "false", "zone": zone_name}
            )
            if principal_data[0] != 200:
                # When strict is True and we cannot find the user. Do not continue toward the root at /ifs
                # If we are at the System zone and we cannot find the user, the user does not exist.
                if strict or zone_name == "System":
                    break
            else:
                principal_entry = {"ts": 1, "name": principal_data[2][base_type][0]["name"]}
                break
        if add_to_cache:
            self.lock.acquire()
            next_ts = self._get_cache_ts()
            if not principal_entry:
                principal_entry = {"ts": next_ts, "name": principal}
            else:
                principal_entry["ts"] = next_ts
            for zone_name in add_to_cache:
                self.zone_auth_cache[zone_name][principal] = principal_entry
            self.zone_auth_cache_size += len(add_to_cache)
            self.lock.release()
            self.expire_cache()
        principal = principal_entry["name"]
        return principal

    def get_user_name(self, principal, path, strict=False, zone_list=None):
        principal = str(principal).upper()
        if not self.principal_check.match(principal):
            principal = "UID:" + principal
        return self.get_principal_name(principal, path, GET_AUTH_TYPE_USER, strict=strict, zone_list=zone_list)

    def get_zones_for_path(self, path):
        zones = self.path_zone_cache.get(path, [])
        if zones:
            return zones
        self.lock.acquire()
        # If no existing entry exists, get the relevant access zone names, then cache the result
        max_depth = self.zone_path_depths[0]
        path_parts = path.split("/")
        path_parts = path_parts[0:max_depth + 1]
        path_len = len(path_parts) - 1
        for depth in self.zone_path_depths:
            test_path = "/".join(path_parts[0:depth + 1])
            if test_path in self.path_depth_zone_name_map[depth]:
                zones.append(self.path_depth_zone_name_map[depth][test_path])
        self.path_zone_cache[path] = zones
        self.path_zone_cache_fifo.append(path)
        # Limit the cache size by deleting the oldest items
        while len(self.path_zone_cache_fifo) > self.path_zone_cache_len:
            key = self.path_zone_cache_fifo.pop()
            del self.path_zone_cache[key]
        self.lock.release()
        return zones


# Instantiate a global auth cache object
auth_cache = GetPrincipalName()


def translate_user_group_perms(root, filename, file_info, fd=None, name_lookup=True, zone_list=None):
    # Populate the perms_user and perms_group fields from the available SID and UID/GID data
    # Translate the numeric values into human readable user name and group names if possible
    # TODO: Add translation to names from SID/UID/GID values
    if file_info["perms_unix_uid"] == 0xFFFFFFFF or file_info["perms_unix_gid"] == 0xFFFFFFFF:
        LOG.debug({"msg": "UID/GID of -1. Using internal security owner call", "file_path": root})
        # If the UID/GID is set to 0xFFFFFFFF then on cluster, the UID/GID does not exist and we have a SID owner
        if not fd:
            full_path = os.path.join(root, filename)
            try:
                fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
            except:
                LOG.warning({"msg": "Unable to get file descriptor to translate user/group", "path": full_path})
                raise
        file_info["perms_user"] = iattr.get_ifs_sec_owner(fd)
        file_info["perms_group"] = iattr.get_ifs_sec_group(fd)
    if "perms_acl_user" in file_info:
        file_info["perms_user"] = file_info["perms_acl_user"]
    elif "perms_user" not in file_info:
        file_info["perms_user"] = "UID:" + str(file_info["perms_unix_uid"])
    if "perms_acl_group" in file_info:
        file_info["perms_group"] = file_info["perms_acl_group"]
    elif "perms_group" not in file_info:
        file_info["perms_group"] = "GID:" + str(file_info["perms_unix_gid"])
    if name_lookup:
        file_info["perms_user"] = auth_cache.get_user_name(file_info["perms_user"], root, zone_list=zone_list)
        file_info["perms_group"] = auth_cache.get_group_name(file_info["perms_group"], root, zone_list=zone_list)
        aces = file_info.get("perms_acl_aces", [])
        for i in range(len(aces)):
            principal_perms = aces[i].split(" ", 1)
            name = auth_cache.get_user_name(principal_perms[0], root, zone_list=zone_list)
            aces[i] = name + " " + principal_perms[1]
