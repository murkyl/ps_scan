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
]
# fmt: on
import logging
import os
import threading
import time

import libs.papi_lite as papi_lite


GET_AUTH_TYPE_AUTO = 0
GET_AUTH_TYPE_GROUP = 1
GET_AUTH_TYPE_USER = 2
LOG = logging.getLogger(__name__)
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
        self.zone_name_cache = {}
        self.zone_path_depths = []
        self.zone_auth_cache = {}
        self._init_access_zone_list()

    def _init_access_zone_list(self):
        data = self.papi_handle.rest_call(URI_ACCESS_ZONES, "GET")
        az_path_lengths = {}
        if data[0] != 200:
            raise ({"msg": "Error occurred while trying to get cluster access zone list", "err": data})
        for zone in data[2]["zones"]:
            path_parts = zone["path"].split("/")
            path_depth = len(path_parts) - 1
            if path_depth not in self.zone_name_cache:
                self.zone_name_cache[path_depth] = {}
            self.zone_name_cache[path_depth][zone["path"]] = str(zone["name"])
            key = zone["name"]
            self.zone_auth_cache[zone["name"]] = dict(WELLKNOWN_SID_TABLE)
        self.zone_path_depths = sorted(self.zone_name_cache.keys(), reverse=True)

    def get_group_name(self, principal, path, strict=False):
        return self.get_principal_name(principal, path, GET_AUTH_TYPE_GROUP, strict)

    def get_principal_name(self, principal, path, principal_type=GET_AUTH_TYPE_AUTO, strict=False):
        principal = str(principal).upper()
        if principal_type not in [GET_AUTH_TYPE_GROUP, GET_AUTH_TYPE_USER]:
            # Try and figure out the principal name by the principal string:
            if "GID" in principal:
                principal_type = GET_AUTH_TYPE_GROUP
            else:
                # Default to trying the user type
                principal_type = GET_AUTH_TYPE_USER
        if principal_type == GET_AUTH_TYPE_USER:
            base_type = "users"
            base_uri = URI_AUTH_USER
        elif principal_type == GET_AUTH_TYPE_GROUP:
            base_type = "groups"
            base_uri = URI_AUTH_GROUP
        else:
            raise Exception({"msg": "Unknown principal type", "type": principal_type})
        add_entry_to_name_cache = []
        path_base_end = os.path.split(path)
        path_parts = path_base_end[0].split("/")
        len_path_parts = len(path_parts) - 1
        for path_depth in self.zone_path_depths:
            if path_depth > len_path_parts:
                continue
            test_path = "/".join(path_parts[0 : path_depth + 1])
            zone_name = self.zone_name_cache[path_depth].get(test_path)
            if not zone_name:
                continue
            name_cache = self.zone_auth_cache[zone_name]
            principal_entry = name_cache.get(principal)
            add_entry_to_name_cache.append(name_cache)
            if not principal_entry:
                principal_data = self.papi_handle.rest_call(
                    base_uri + "/" + principal, "GET", query_args={"query_member_of": "false", "zone": zone_name}
                )
                if principal_data[0] != 200:
                    # When strict is True, if we made it this far, then we foudn the longest matching path already
                    # If the user doesn't exist at this level, don't continue to go toward the root at /ifs
                    # We short circuit future lookups by saving the passed in principal as the name and returning it
                    if strict:
                        name_cache[principal] = principal
                        break
                    continue
                principal_entry = {"ts": time.time(), "name": principal_data[2][base_type][0]["name"]}
                self.lock.acquire()
                for cache in add_entry_to_name_cache:
                    cache[principal] = principal_entry
                self.lock.release()
            return principal_entry["name"]
        return principal

    def get_user_name(self, principal, path, strict=False):
        return self.get_principal_name(principal, path, GET_AUTH_TYPE_USER, strict)
