#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__copyright__ = """Copyright 2020-2024 Andrew Chung
Permission is hereby granted, free of charge, to any person obtaining a copy of 
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to 
use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies 
of the Software, and to permit persons to whom the Software is furnished to do 
so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE 
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, 
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE 
SOFTWARE."""
__title__         = "papi_lite"
__version__       = "2.3.0"
__date__          = "06 June 2024"
__license__       = "MIT"
__author__        = "Andrew Chung"
__maintainer__    = "Andrew Chung"
__email__         = "acchung@gmail.com"
__all__           = [
    "papi_lite",
]
# fmt: on


import collections
import json
import logging
import platform
import re
import ssl

try:
    import httplib as api
except ImportError:
    import http.client as api
try:
    from urlparse import urlunsplit
    from urlparse import urljoin
    from urllib import urlencode
except ImportError:
    from urllib.parse import urlunsplit
    from urllib.parse import urljoin
    from urllib.parse import urlencode
if "OneFS" in platform.system():
    import isi.rest
try:
    basestring
except:
    basestring = str


DEFAULT_API_TIMEOUT = 300
API_PAPI = 1
API_RAN = 2
API_SUPPORT = 4
MAX_SESSION_RETRY = 5
TIMEOUT = 10
URL_PAPI_SESSION = "/session/1/session"
URL_PAPI_PLATFORM_PREFIX = "/platform/%s"
URL_RAN_PLATFORM_PREFIX = "/namespace/%s"
URL_SUPPORT_PLATFORM_PREFIX = "/remote-service/%s"
LOG = logging.getLogger(__name__)


class papi_lite:
    """Initialize the PAPI lite interface

    user: Text string for the user accessing the API. Used only for HTTP connections.
    password: Text string for the user password. Used only for HTTP connections.
    server: Text string for server URL without the https:// prefix. Just provide the FQDN/IP address and port. e.g. a.b.c.d:8080. Used only for HTTP connections.
    ignorecert: Boolean to disable certificate checking. Used only for HTTP connections.
    oncluster: Boolean or set to None. With a boolean value this will force either usage of internal or not. When set to None, the script will prefer internal API versus HTTP.
    """

    def __init__(
        self,
        user=None,
        password=None,
        server=None,
        ignorecert=True,
        oncluster=None,
    ):
        self.user = user
        self.password = password
        self.server = server
        self.ignorecert = ignorecert
        self.oncluster = oncluster
        self.session = None
        self.csrf = None
        self.ctx = None
        if self.oncluster is None:
            self.oncluster = "OneFS" in platform.system()
        self.init_http_context()

    def init_http_context(self):
        if self.oncluster:
            return
        self.ctx = ssl.create_default_context()
        if self.ignorecert:
            self.ctx.check_hostname = False
            self.ctx.verify_mode = ssl.CERT_NONE

    def create_http_session(self):
        """Connects to a OneFS cluster and gets a PAPI session cookie"""
        if self.oncluster:
            # When running on cluster skip HTTP session creation
            return
        # Cleanup any existing HTTP session
        self.delete_http_session()
        headers = {"Content-type": "application/json", "Accept": "application/json"}
        conn = api.HTTPSConnection(self.server, timeout=TIMEOUT, context=self.ctx)
        # Always ask for both platform and namespace access
        data = json.dumps(
            {
                "username": self.user,
                "password": self.password,
                "services": ["platform", "namespace"],
            }
        )
        try:
            conn.request("POST", URL_PAPI_SESSION, data, headers)
        except IOError as ioe:
            if ioe.errno == 61:
                raise Exception(
                    "Could not connect to the server. Check the URL including port number. Port 8080 is default."
                )
            raise
        except Exception:
            raise
        resp = conn.getresponse()
        msg = resp.read()
        LOG.debug("Response status code: %s" % resp.status)
        LOG.debug("Response: %s" % msg)
        LOG.debug("Headers: %s" % resp.getheaders())
        if resp.status != 200 and resp.status != 201:
            try:
                err_msg = json.loads(msg)["message"]
            except:
                err_msg = "Error creating PAPI session"
            raise Exception(err_msg)
        cookies = resp.getheader("set-cookie").split(";")
        LOG.debug("Cookies line: %s" % cookies)
        session = None
        csrf = None
        for item in cookies:
            if "isisessid=" in item:
                m = re.search(r".*(isisessid=[^;\s]*)", item)
                if m:
                    session = m.group(1).strip()
            if "isicsrf=" in item:
                m = re.search(r".*(isicsrf=[^;]*)", item)
                if m:
                    csrf = m.group(1).strip()
        LOG.debug("Session: %s, CSRF: %s" % (session, csrf))
        conn.close()
        self.session, self.csrf = collections.namedtuple("papi_session", ["session_id", "csrf"])(session, csrf)
        if self.csrf:
            self.csrf = self.csrf.split("=")[1]

    def delete_http_session(self):
        """Cleanup any existing HTTP session"""
        if self.session and not self.oncluster:
            # TODO: Add code to disconnect session
            pass
        self.session = None
        self.csrf = None

    def rest_call(
        self,
        url,
        method=None,
        query_args=None,
        headers=None,
        body=None,
        timeout=DEFAULT_API_TIMEOUT,
        api_type=API_PAPI,
        raw=False,
    ):
        """Perform a REST call either using HTTPS or when run on an Isilon cluster,
        use the internal PAPI socket path or internal RAN socket path

        self: Object state
        url: Can be a full URL string with slashes or an array of strings with no slashes
        method: HTTP method. GET, POST, PUT, DELETE, etc. Default: GET
        query_args: Dictionary of key value pairs to be appended to the URL
        headers: Optional dictionary used to override HTTP headers
        body: Data to be put into the request body
        timeout: Number of seconds to wait for command to complete. Only used for the internal REST call
        api_type: Set to API_PAPI for PAPI calls or API_RAN for RAN calls.

        When using the RAN API, the URL must not include the '/namespace' prefix. The root URL would be: '/ifs'
        """
        resume = True
        response_list = []
        method = method or "GET"
        query_args = query_args or {}
        headers = headers or {}
        body = body or ""
        remote_url = url
        LOG.debug(
            "REST Call params: Method: %s | URL: %s | Query Args: %s" % (method, remote_url, json.dumps(query_args))
        )
        if isinstance(url, basestring):
            remote_url = [str(x) for x in url.split("/") if x]
        if self.oncluster:
            LOG.debug("On cluster query")
            if api_type == API_RAN:
                # The RAN internal call requires a special header to be sent and the "namespace" component to be added to the URL
                headers["SCRIPT_NAME"] = "/namespace"
                remote_url.insert(0, "namespace")
            socket_path = (
                isi.rest.PAPI_SOCKET_PATH * (api_type == API_PAPI)
                or isi.rest.OAPI_SOCKET_PATH * (api_type == API_RAN)
                or isi.rest.RSAPI_SOCKET_PATH * (api_type == API_SUPPORT)
            )
            while resume:
                data = isi.rest.send_rest_request(
                    socket_path=socket_path,
                    method=method,
                    uri=remote_url,
                    query_args=query_args,
                    headers=headers,
                    body=body,
                    timeout=timeout,
                )
                if data:
                    LOG.debug("REST call response: %s" % data[0])
                    try:
                        resume = json.loads(data[2])["resume"]
                        LOG.debug("Resume key: %s" % resume)
                        query_args = {"resume": str(resume) or ""}
                    except:
                        resume = False
                    response_list.append(data)
                else:
                    resume = False
                    LOG.warning("Error occurred getting data from cluster. URL: %s" % remote_url)
        else:
            LOG.debug("HTTPS query")
            conn = None
            max_retry = MAX_SESSION_RETRY
            url_prefix = URL_PAPI_PLATFORM_PREFIX * (api_type == API_PAPI) or URL_RAN_PLATFORM_PREFIX
            try:
                while resume:
                    if not self.session:
                        self.create_http_session()
                    headers["Cookie"] = self.session
                    if self.csrf:
                        headers["X-CSRF-Token"] = self.csrf
                        headers["Referer"] = "https://" + self.server
                    headers["Content-type"] = "application/json"
                    headers["Accept"] = "application/json"
                    LOG.debug("Sending headers: %s" % headers)
                    url = urlunsplit(
                        [
                            "",
                            "",
                            url_prefix % "/".join(remote_url),
                            urlencode(query_args),
                            None,
                        ]
                    )
                    LOG.debug("Method: %s" % method)
                    LOG.debug("URL: %s" % url)
                    LOG.debug("Headers: %s" % headers)
                    # Send request over HTTPS
                    conn = api.HTTPSConnection(self.server, context=self.ctx)
                    conn.request(method, url, body, headers=headers)
                    resp = conn.getresponse()
                    LOG.debug("HTTPS Response code: %d" % resp.status)
                    if resp and 200 <= resp.status < 300:
                        LOG.debug("HTTPS call response: %s" % resp.status)
                        data = resp.read()
                        LOG.debug("Raw data: %s" % data)
                        try:
                            resume_check = json.loads(data)
                        except:
                            resume_check = {}
                        resume = resume_check.get("resume", None)
                        LOG.debug("Resume key: %s" % resume)
                        query_args = {"resume": str(resume) or ""}
                        response_list.append([resp.status, resp.reason, data])
                    elif resp.status == 401:
                        # Our session token has expired so we will re-negotiate a new session
                        self.session = None
                        max_retry -= 1
                        if max_retry:
                            continue
                        raise Exception(
                            "Failed to re-create session token after %d tries. Last try error code: %d"
                            % (MAX_SESSION_RETRY, resp.status)
                        )
                    else:
                        resume = False
                        raise Exception("Error occurred getting data from cluster. Error code: %d" % resp.status)
                if conn:
                    conn.close()
            except IOError as ioe:
                if ioe.errno == 111:
                    raise Exception("Could not connect to server: %s. Check address and port." % state["SERVER"])
                raise
        try:
            # Combine multiple responses into 1
            response = response_list[0]
            if response[2]:
                json_data = json.loads(response[2])
            else:
                json_data = {}
        except Exception as e:
            if not raw:
                json_data = {}
                response = [500, None]
            else:
                json_data = response_list[0]
                response = [0, None]
        if len(response_list) > 1:
            keys = json_data.keys()
            try:
                keys.remove("total")
            except:
                pass
            keys.remove("resume")
            if len(keys) > 1:
                raise Exception("More keys remaining in REST call response than we expected: %s" % keys)
            key = keys[0]
            for i in range(1, len(response_list)):
                json_data[key] = json_data[key] + json.loads(response_list[i][2])[key]
        return response[0], response[1], json_data
