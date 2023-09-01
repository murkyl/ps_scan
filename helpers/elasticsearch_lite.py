#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "elasticsearch_lite"
__version__       = "1.0.0"
__date__          = "12 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "ElasticsearchLite",
]
# fmt: on
import base64
import json
import ssl

try:
    import http.client as http_conn
except:
    import httplib as http_conn
try:
    import urllib.parse

    urlencode = urllib.parse.urlencode
except:
    import urllib

    urlencode = urllib.urlencode


def get_basic_auth_header(user, password):
    user_pass_str = "{u}:{p}".format(u=user, p=password)
    user_pass_str = user_pass_str.encode("ascii")
    token = base64.b64encode(user_pass_str)
    token = token.decode("ascii")
    return "Basic {t}".format(t=token)


class ElasticsearchLite:
    def __init__(self):
        self.conn = None
        self.endpoint = None
        self.headers = {}
        self.password = None
        self.use_https = True
        self.username = None

    def _simple_es_request(self, url_base, op="GET", body_str=None, index_name=None, query=None):
        if not self.conn:
            self.connect()
        if index_name:
            url_base = "{idx}/{url}".format(idx=index_name, url=url_base)
        if query:
            if not (isinstance(query, str) or isinstance(query, unicode)):
                url_base += "?" + urlencode(query)
            else:
                url_base += "?" + query
        self.conn.request(op, url_base, body_str, self.headers)
        resp = self.conn.getresponse()
        return json.loads(resp.read())

    def bulk(self, body_str, index_name=None):
        if body_str and body_str[-1] != "\n":
            body_str += "\n"
        return self._simple_es_request("_bulk", op="POST", body_str=body_str, index_name=index_name)

    def connect(self):
        self.validate_options()
        if self.use_https:
            self.conn = http_conn.HTTPSConnection(self.endpoint, context=ssl._create_unverified_context())
        else:
            self.conn = http_conn.HTTPConnection(self.endpoint)
        auth_header = None
        if self.password and self.username:
            auth_header = get_basic_auth_header(self.username, self.password)
        self.headers = {
            "Content-Type": "application/json",
        }
        if auth_header:
            self.headers["Authorization"] = auth_header

    def create_index(self, index_name, query=None, settings=None, aliases=None, mapping=None):
        body = {}
        body_str = None
        if settings:
            body["settings"] = settings
        if aliases:
            body["aliases"] = aliases
        if mapping:
            body["mappings"] = mapping
        if body:
            body_str = json.dumps(body)
        return self._simple_es_request(index_name, op="PUT", body_str=body_str)

    def delete_index(self, index_name, query=None):
        return self._simple_es_request(index_name, op="DELETE", query=query)

    def disconnect(self):
        if not self.conn:
            return
        self.conn.close()
        self.conn = None

    def forcemerge(self, index_name=None, query=None):
        return self._simple_es_request("_forcemerge", op="POST", index_name=index_name, query=query)

    def flush(self, index_name=None):
        return self._simple_es_request("_flush", op="POST", index_name=index_name)

    def info(self):
        return self._simple_es_request("_xpack")

    def post_document(self, body_str, index_name=None, query=None):
        if isinstance(body_str, dict):
            body_str = json.dumps(body_str)
        return self._simple_es_request("_doc", op="POST", body_str=body_str, index_name=index_name, query=query)

    def search(self, body_str, index_name=None):
        if isinstance(body_str, dict):
            body_str = json.dumps(body_str)
        return self._simple_es_request("_search", body_str=body_str, index_name=index_name)

    def update_index_settings(self, body_str, index_name=None, query=None):
        if isinstance(body_str, dict):
            body_str = json.dumps(body_str)
        return self._simple_es_request("_settings", op="PUT", body_str=body_str, index_name=index_name, query=query)

    def validate_options(self):
        if not self.endpoint:
            raise ValueError("Endpoint required")
        self.endpoint = self.endpoint.strip()
        if self.endpoint.startswith("http://"):
            self.use_https = False
            self.endpoint = self.endpoint[7:]
        elif self.endpoint.startswith("https://"):
            self.use_https = True
            self.endpoint = self.endpoint[8:]
