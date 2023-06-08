#!/usr/bin/env python
# coding: utf-8
"""
Methods to help with parsing and generating ndjson files for Kibana visualizations
"""
# fmt: off
__title__         = "vis_gen_helpers"
__version__       = "1.0.0"
__date__          = "08 June 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__           = [
    "clear_access_zone_filter",
    "clear_vis_file_categories",
    "generate_access_zone_filter_ndjson",
    "generate_file_category_ndjson",
    "get_dict_value_and_path",
    "get_ps_access_zone_list",
    "update_lens_access_zone_filter",
    "update_vis_add_file_categories",
]
# fmt: on


import datetime
import json
import re
import sys
import traceback
import helpers.papi_lite as papi_lite


URI_ACCESS_ZONE_LIST = "/zones"


def _get_utc_now():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%Z")


def clear_access_zone_filter_ndjson(template_array):
    ndjson = []
    for line in template_array:
        try:
            json_data = json.loads(line)
            if json_data.get("type") != "lens":
                ndjson.append(line)
                continue
            # Update the lens state with a new set of filters
            visState_dict = json_data["attributes"]["state"]
            update_lens_access_zone_filter(visState_dict, [], add_other=False)
            # Encode the updated dictionary into JSON
            ndjson.append(json.dumps(json_data, sort_keys=True))
        except Exception as e:
            raise Exception("Exception generating access zone overview: {err}\n".format(err=traceback.format_exc()))
            ndjson.append(line)
    return ndjson


def clear_vis_file_categories_ndjson(template_array):
    ndjson = []
    for line in template_array:
        try:
            json_data = json.loads(line)
            if json_data.get("type") != "visualization":
                ndjson.append(line)
                continue
            # Update the visualization state embeded JSON with the categories in categories_dict
            visState_dict = json.loads(json_data["attributes"]["visState"])
            update_vis_add_file_categories(visState_dict, {}, add_other=False)
            json_data["attributes"]["visState"] = json.dumps(visState_dict, sort_keys=True)
            # Encode the updated dictionary into JSON
            ndjson.append(json.dumps(json_data, sort_keys=True))
        except Exception as e:
            raise Exception("Exception generating file categories: {err}\n".format(err=traceback.format_exc()))
            ndjson.append(line)
    return ndjson


def generate_access_zone_filter_ndjson(template_array):
    ndjson = []
    az_list = get_ps_access_zone_list()
    for line in template_array:
        try:
            json_data = json.loads(line)
            if json_data.get("type") != "lens":
                ndjson.append(line)
                continue
            # Update the lens state with a new set of filters
            visState_dict = json_data["attributes"]["state"]
            update_lens_access_zone_filter(visState_dict, az_list)
            # Encode the updated dictionary into JSON
            ndjson.append(json.dumps(json_data, sort_keys=True))
        except Exception as e:
            raise Exception("Exception generating access zone overview: {err}\n".format(err=traceback.format_exc()))
            ndjson.append(line)
    return ndjson


def generate_file_category_ndjson(template_array, categories_dict):
    ndjson = []
    for line in template_array:
        try:
            json_data = json.loads(line)
            if json_data.get("type") != "visualization":
                ndjson.append(line)
                continue
            # Update the visualization state embeded JSON with the categories in categories_dict
            visState_dict = json.loads(json_data["attributes"]["visState"])
            update_vis_add_file_categories(visState_dict, categories_dict)
            json_data["attributes"]["visState"] = json.dumps(visState_dict, sort_keys=True)
            # Encode the updated dictionary into JSON
            ndjson.append(json.dumps(json_data, sort_keys=True))
        except Exception as e:
            raise Exception("Exception generating file categories: {err}\n".format(err=traceback.format_exc()))
            ndjson.append(line)
    return ndjson


def get_dict_value_and_path(dict_obj, path):
    """
    This function walks down a dictionary object and returns the value at the specified point in the path.
    The function also returns the path that it took to reach the value. In case a wildcard is used and a list entry is
        traversed, the list index will be returned in the path in the form ~<number>~.

    The path variable can be a string of the form "key1/key2/key3" or an array of keys like [key1, key2, key3]
    The keys can be numeric, text, the special character '*', the special string '*[<number>]', or the special string
        '=<string>'
    For the numeric and text keys, these are simply used to navigate down the object
    For the special character '*', this represents a wildcard and this will work on a part of the dictionary that is a
        list or a dictionary. It will try each list entry or dictionary key and try and return the first valid value
        that matches the remainder of the path.
    For the special string "*[<number>]" the method will assume that the object at that part of the path is a list and
        it will choose the index that is represented by the number between the square brackets.
    For the special string "=<string>", the method will check if the string matches the value at that part in the
        dictionary. If it matches then the method returns the value and the path else it will return the no match tuple.

    If the method fails to find a matching path in the dictionary, the tuple (None, None) will be returned.

    Example string using a wildcard: /users/*/employed
    On the following dictionary object:
    {
      "users": [
        {
          "first": "User1",
          "last": "Last name",
        },
        {
          "first": "User2",
          "last": "Last name2",
          "employed": True
        }
      ]
    }
    The function will return the tuple: (True, "users/~2~/employed")
    """
    if not isinstance(path, list):
        path = path.split("/")
    position = dict_obj
    last_idx = None
    for i in range(len(path)):
        if path[i] == "*":
            if isinstance(position, dict):
                keys = list(position.keys())
                for j in range(len(keys)):
                    val, found_path = get_dict_value_and_path(position[keys[j]], path[i + 1 :])
                    if val and found_path:
                        return val, path[0:i] + [keys[j]] + found_path
            elif isinstance(position, list):
                for j in range(len(position)):
                    val, found_path = get_dict_value_and_path(position[j], path[i + 1 :])
                    if val and found_path:
                        return val, path[0:i] + ["~%s~" % j] + found_path
                return None, None
            else:
                raise Exception("ERROR: Got * but object path is not a list or dict: %s" % path)
                return None, None
        elif re.match(r"\*\[\d+\]", path[i]):
            match = re.match(r"\*\[(\d+)\]", path[i])
            fixed_index = int(match.group(1))
            if not isinstance(position, list):
                raise Exception("ERROR: Path specified a specific list index but list object not found: %s" % path)
            val, found_path = get_dict_value_and_path(position[fixed_index], path[i + 1 :])
            if val and found_path:
                return val, path[0:i] + ["~%s~" % fixed_index] + found_path
            return None, None
        elif re.match(r"=.*", path[i]):
            if position == path[i][1:]:
                return position, path
            return None, None
        if path[i] not in position:
            return None, None
        last_idx = i
        position = position.get(path[i])
    return position, path


def get_ps_access_zone_list():
    zone_list = []
    papi = papi_lite.papi_lite()
    data = papi.rest_call(URI_ACCESS_ZONE_LIST, "GET")
    if data[0] != 200:
        raise Exception("Error in PAPI request to {url}:\n{err}".format(err=str(data), url=URI_ACCESS_ZONE_LIST))
    for zone in data[2].get("zones"):
        zone_list.append({"name": zone["name"], "path": zone["path"]})
    zone_list = sorted(zone_list, key=lambda x: x["path"])
    return zone_list


def update_lens_access_zone_filter(visState_dict, az_list, add_other=True):
    value, path = get_dict_value_and_path(
        visState_dict, "datasourceStates/formBased/layers/*/columns/*/operationType/=filters"
    )
    if not value and not path:
        raise ValueError('Could not find a section with "operationType=filters" in the passed in dictionary')
    # Fix up the returned path and discard the last 2 parts and replace it with the section to perform the replacement
    path = path[0:-2]
    path.append("params")
    all_paths = []
    filters = []
    for az in az_list:
        entry = {
            "input": {"language": "kuery", "query": "file_path :{path}/*".format(path=az["path"].rstrip("/"))},
            "label": az["name"],
        }
        filters.append(entry)
        all_paths.append(az["path"].rstrip("/") + "/*")
    if add_other:
        entry = {
            "input": {
                "language": "kuery",
                "query": "file_path :/ifs/* and not {not_paths}".format(not_paths=" and not ".join(all_paths)),
            },
            "label": "System (excluding all other access zones)",
        }
        filters.append(entry)
    cur_obj = visState_dict
    for key in path:
        cur_obj = cur_obj[key]
    cur_obj["filters"] = filters
    return visState_dict


def update_vis_add_file_categories(visState_dict, categories_dict, add_other=True):
    aggs = visState_dict.get("aggs")
    if not aggs:
        return visState_dict
    for item in aggs:
        if item["type"] != "filters":
            continue
        filters = []
        all_extensions = []
        for category in categories_dict:
            all_extensions.extend(categories_dict[category])
            entry = {
                "input": {
                    "language": "lucene",
                    "query": "file_ext:/\.?({ext_list})/".format(ext_list="|".join(categories_dict[category])),
                },
                "label": category,
            }
            filters.append(entry)
        if add_other:
            entry = {
                "input": {
                    "language": "lucene",
                    "query": "-file_ext:/\.?({ext_list})/".format(ext_list="|".join(all_extensions)),
                },
                "label": "Others",
            }
            filters.append(entry)
        item["params"]["filters"] = filters
    return visState_dict
