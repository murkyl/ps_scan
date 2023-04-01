#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "elasticsearchlite"
__version__       = "1.0.0"
__date__          = "01 April 2023"
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

try:
    import http.client as http_conn
except:
    import httplib as http_conn


def get_basic_auth_header(user, password):
    token = base64.b64encode("{u}:{p}".format(u=user, p=password)).encode("utf-8").decode("ascii")
    return "Basic {t}".format(t=token)


class ElasticsearchLite:
    def __init__(self):
        self.conn = None
        self.endpoint = None
        self.headers = {}
        self.password = None
        self.use_https = True
        self.username = None

    def connect(self):
        self.validate_options()
        if self.use_https:
            self.conn = http_conn.HTTPSConnection(self.endpoint)
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

    def bulk(self, body_str, index_name=None):
        if not self.conn:
            self.connect()
        url = "_bulk"
        if index_name:
            url = "{idx}/".format(idx=index_name) + url

        if body_str[-1] != "\n":
            body_str += "\n"
        self.conn.request("POST", url, body_str, self.headers)
        resp = self.conn.getresponse()
        return json.loads(resp.read())

    def create_index(self, index_name, query=None, settings=None, aliases=None, mapping=None):
        if not self.conn:
            self.connect()
        body = {}
        if settings:
            body["settings"] = settings
        if aliases:
            body["aliases"] = aliases
        if mapping:
            body["mappings"] = mapping
        if body:
            str_body = json.dumps(body)
        else:
            str_body = None
        self.conn.request("PUT", index_name, str_body, self.headers)
        resp = self.conn.getresponse()
        return json.loads(resp.read())

    def info(self):
        if not self.conn:
            self.connect()
        self.conn.request("GET", "_xpack", headers=self.headers)
        resp = self.conn.getresponse()
        return json.loads(resp.read())

    def search(self, body_str, index_name=None):
        if not self.conn:
            self.connect()
        url = "_search"
        if index_name:
            url = "{idx}/".format(idx=index_name) + url
        self.conn.request("GET", url, body_str, headers=self.headers)
        resp = self.conn.getresponse()
        return json.loads(resp.read())

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
