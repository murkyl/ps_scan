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
import io
import gzip
import json
import logging
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


LOG = logging.getLogger(__name__)


def get_basic_auth_header(user, password):
    user_pass_str = "{u}:{p}".format(u=user, p=password)
    user_pass_str = user_pass_str.encode("ascii")
    token = base64.b64encode(user_pass_str)
    token = token.decode("ascii")
    return "Basic {t}".format(t=token)


class ElasticsearchLite:
    def __init__(self):
        self.conn = None
        self.conn_endpoint = None
        self.endpoint = None
        self.headers = {}
        self.password = None
        self.use_https = True
        self.username = None

    def _simple_es_request(self, url_base, op="GET", body_str=None, index_name=None, query=None, headers={}):
        if not self.conn:
            self.connect()
        if index_name:
            url_fmt = "/{idx}/{url}"
        else:
            url_fmt = "/{url}"
        url_base = url_fmt.format(idx=index_name, url=url_base)
        if query:
            if not (isinstance(query, str) or isinstance(query, unicode)):
                url_base += "?" + urlencode(query)
            else:
                url_base += "?" + query
        request_headers = dict(self.headers)
        request_headers.update(headers)
        self.conn.request(op, url_base, body_str, request_headers)
        resp = self.conn.getresponse()
        if resp.status >= 200 and resp.status < 300:
            return json.loads(resp.read())
        self.conn = None
        return {"status": resp.status, "error": resp.reason}

    def bulk(self, body_str, index_name=None, compress=0):
        new_headers = {}
        if body_str and body_str[-1] != "\n":
            body_str += "\n"
        # 0: No compression, 1: Fastest, 9: Slowest
        if compress > 9:
            compress = 9
        if compress > 0:
            buffer = io.BytesIO()
            with gzip.GzipFile(mode="wb", compresslevel=compress, fileobj=buffer) as gz_file:
                gz_file.write(body_str)
            body_str = buffer.getvalue()
            new_headers["Content-Encoding"] = "gzip"
        new_headers["Content-Length"] = len(body_str)
        return self._simple_es_request(
            "_bulk", op="POST", body_str=body_str, index_name=index_name, headers=new_headers
        )

    def connect(self):
        self.validate_options()
        if self.use_https:
            self.conn = http_conn.HTTPSConnection(self.conn_endpoint, context=ssl._create_unverified_context())
        else:
            self.conn = http_conn.HTTPConnection(self.conn_endpoint)
        auth_header = None
        if self.password and self.username:
            auth_header = get_basic_auth_header(self.username, self.password)
        self.headers = {
            "Content-Type": "application/json",
            "Accept": "*/*",
            "Agent": "elasticsearch_lite/{ver}".format(ver=__version__),
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
        # TODO: Check if the connection is still alive instead of always closing and assuming the connection is dead
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None
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
            self.conn_endpoint = self.endpoint[7:]
        elif self.endpoint.startswith("https://"):
            self.use_https = True
            self.conn_endpoint = self.endpoint[8:]
        else:
            raise ValueError("Invalid endpoint: {endpoint}".format(endpoint=self.endpoint))
