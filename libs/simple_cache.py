#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "simple_cache"
__version__       = "1.0.0"
__date__          = "25 September 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "SimpleCache",
]
# fmt: on
import gzip
import hashlib
import json
import logging
import tempfile
import threading
import time


DEFAULT_CACHE_CLEANUP_INTERVAL = 60  # Try to cleanup the cache every 60 seconds
DEFAULT_CACHE_SIZE_MAX = 1024**3  # Default max cache size is 1 GiB
DEFAULT_CACHE_TIMEOUT = 1800  # Time in seconds after which a continuation token is considered expired and cleaned up
LOG = logging.getLogger(__name__)


class SimpleCache:
    def __init__(self, args=None):
        args = args or {}
        self.lock = threading.Lock()
        self.cache = {}
        # Number of time the cache has been used
        self.cache_count = 0
        # Number of current cache entries that are files
        self.cached_files = 0
        # Number of current cache entries that are in memory
        self.cached_memory = 0
        # Number of times we have had to write an entry to a file
        self.cache_overflow_count = 0
        # Size of the current cache in bytes
        self.cache_size = 0
        # Maximum size of the in memory cache in bytes
        self.cache_size_max = args.get("cache_size_max", DEFAULT_CACHE_SIZE_MAX)
        # Number of seconds until a cached entry expires
        self.cache_timeout = args.get("cache_timeout", DEFAULT_CACHE_TIMEOUT)
        # Number of seconds between cache cleanups
        self.cache_clean_interval = args.get("cache_cleanup_interval", DEFAULT_CACHE_CLEANUP_INTERVAL)
        # Cleanup timer thread
        self.timer = threading.Timer(self.cache_clean_interval, self._clean_cache_timeout)
        self.timer.daemon = True
        self.timer.start()

    def __del__(self):
        if self.timer:
            self.timer.cancel()
            self.timer = None
        if self.cache:
            self._clean_cache(force=True)
            self.cache = {}

    def _clean_cache(self, force=False):
        """Checks each cache entry to see if it has expired and cleans up expired entries

        Parameters
        ----------
        force: <boolean> - When set to true, unexpired entries are also cleaned up

        Returns
        ----------
        No return value
        """
        LOG.debug({"msg": "Cleaning cache", "cache_entries": len(self.cache.keys()), "cache_size": self.cache_size})
        now = time.time()
        # Grab lock
        try:
            self.lock.acquire()
            for token in list(self.cache.keys()):
                cache_item = self.cache.get(token, {})
                if cache_item and (cache_item["timeout"] < now or force):
                    # Clean up item
                    if "file" in cache_item:
                        cache_item["file"].close()
                        self.cached_files -= 1
                    else:
                        self.cache_size -= cache_item["data_len"]
                        self.cached_memory -= 1
                    del self.cache[token]
        except Exception as e:
            LOG.exception(e)
        finally:
            self.lock.release()
        # Released lock
        LOG.debug(
            {
                "msg": "Clean cache complete",
                "cache_count": self.cache_count,
                "cache_entries": self.cached_files + self.cached_memory,
                "cache_size": self.cache_size,
                "cache_overflow_count": self.cache_overflow_count,
                "cached_files": self.cached_files,
                "cached_memory": self.cached_memory,
            }
        )

    def _clean_cache_timeout(self):
        self.timer = threading.Timer(self.cache_clean_interval, self._clean_cache_timeout)
        self.timer.daemon = True
        self.timer.start()
        self._clean_cache()

    def add_item(self, item, use_files=True):
        """Adds an item into the cache

        Parameters
        ----------
        item: <object> Item to be stored in the cache
        use_files: <boolean> Set to true if the cache should try and write to disk when cache is full, false otherwise

        Returns
        ----------
        dict - A dictionary with 2 values as follows:
                {
                    "token": <string> String to be used in a subsequent get_item command to retrieve this item
                    "expiration": <int> Epoch time in seconds specifying when the token will expire in the future
                }
        """
        json_data = json.dumps(item)
        comp_data = gzip.zlib.compress(bytes(str(json_data).encode("utf-8")), 9)
        json_data = None
        comp_data_len = len(comp_data)
        cache_entry = {
            "timeout": time.time() + self.cache_timeout,
            "data": comp_data,
            "data_len": comp_data_len,
        }
        if comp_data_len + self.cache_size > self.cache_size_max:
            self._clean_cache()
        if comp_data_len + self.cache_size > self.cache_size_max:
            # If we cannot use a file to store the item, return a None to signal an out of memory situation
            if not use_files:
                return None
            LOG.debug({"msg": "Cache full. Writing item to disk", "len": comp_data_len, "cache_size": self.cache_size})
            # If a cache clean does not free enough space, we need to write this cache item to disk
            tfile = tempfile.TemporaryFile()
            tfile.write(comp_data)
            tfile.seek(0)
            cache_entry.update(
                {
                    "data": None,
                    "file": tfile,
                }
            )
            self.cache_overflow_count += 1
            self.cached_files += 1
            # Pre-subtract 1 from cached_memory. This is adjusted +1 during the add into the cache dictionary
            self.cached_memory -= 1
        # Grab lock
        try:
            self.lock.acquire()
            self.cache_count += 1
            self.cached_memory += 1
            token_string = "{time}{len}{rand}{count}".format(
                time=cache_entry["timeout"],
                len=cache_entry["data_len"],
                rand=random.randint(0, 100000),
                count=self.cache_count,
            )
            token = hashlib.sha256(bytes(str(token_string).encode("utf-8"))).hexdigest()
            self.cache[token] = cache_entry
            self.cache_size += 0 if cache_entry.get("file") else comp_data_len
        except Exception as e:
            LOG.exception(e)
            raise
        finally:
            self.lock.release()
        # Released lock
        LOG.debug(
            {
                "msg": "Caching item",
                "len": comp_data_len,
                "cache_count": self.cache_count,
                "cache_entries": self.cached_files + self.cached_memory,
                "cache_size": self.cache_size,
                "cache_overflow_count": self.cache_overflow_count,
                "cached_files": self.cached_files,
                "cached_memory": self.cached_memory,
            }
        )
        return {"token": token, "expiration": cache_entry["timeout"]}

    def get_item(self, token):
        """Returns the item in the cache to the caller based on the token

        Parameters
        ----------
        token: <string> String token used to locate the cached item

        Returns
        ----------
        <object> The item associated with the token. If the token is invalid or the cached item has expired, then
                None will be returned instead
        """
        # Grab lock
        try:
            self.lock.acquire()
            cache_item = self.cache.get(token)
            if not cache_item:
                return None
            if "file" in cache_item:
                self.cached_files -= 1
            else:
                # Only decrement the cache_size if the cache item is not a file
                self.cache_size -= cache_item["data_len"]
                self.cached_memory -= 1
            del self.cache[token]
        except Exception as e:
            LOG.exception(e)
        finally:
            self.lock.release()
        # Released lock
        if "file" in cache_item:
            comp_data = cache_item["file"].read()
            cache_item["file"].close()
        else:
            comp_data = cache_item["data"]
        cache_item = json.loads(gzip.zlib.decompress(comp_data).decode("utf-8"))
        return cache_item
