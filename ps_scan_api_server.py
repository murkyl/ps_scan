import datetime
import errno
import gzip
import hashlib
import json
import logging
from logging.config import dictConfig
import os
import random
import re
import stat
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.realpath(__file__)), "libs"))
try:
    import urllib.parse

    urlencode = urllib.parse.urlencode
    urlquote = urllib.parse.quote
    urlunquote = urllib.parse.unquote
except:
    import urllib

    urlencode = urllib.urlencode
    urlquote = urllib.quote
    urlunquote = urllib.unquote
try:
    dir(os.scandir)
    use_scandir = 1
except:
    use_scandir = 0

dictConfig(
    {
        "version": 1,
        "formatters": {
            "default": {
                "format": "[%(asctime)s] %(levelname)s in %(module)s: %(message)s",
            }
        },
        "handlers": {
            "wsgi": {
                "class": "logging.StreamHandler",
                "stream": "ext://flask.logging.wsgi_errors_stream",
                "formatter": "default",
            }
        },
        "root": {"level": "DEBUG", "handlers": ["wsgi"]},
    }
)


from helpers.constants import *
import helpers.misc as misc
from libs.flask import Flask
from libs.flask import make_response
from libs.flask import request
from libs.flask import Response
from libs.waitress import serve

try:
    import isi.fs.attr as attr
    import isi.fs.diskpool as dp
    import isi.fs.userattr as uattr
    import libs.onefs_acl as onefs_acl
except:
    pass
try:
    dir(PermissionError)
except:
    PermissionError = Exception
try:
    dir(FileNotFoundError)
except:
    FileNotFoundError = IOError

app = Flask(__name__)
LOG = app.logger
DATA_TYPE_PS = "powerscale"
DATA_TYPE_DISKOVER = "diskover"
DEFAULT_DATA_TYPE = DATA_TYPE_PS
DEFAULT_ITEM_LIMIT = 10000 # Default number of items to return in a single call
DEFAULT_MAX_ITEM_LIMIT = 100000 # Allow up to 100,000 items to be returned in a single call
DEFAULT_CACHE_CLEANUP_INTERVAL = 60 # Try to cleanup the cache every 60 seconds
DEFAULT_CACHE_SIZE_MAX = 1024^4 # Default max cache size is 1 GiB
DEFAULT_CACHE_TIMEOUT = 1800  # Time in seconds after which a continuation token is considered expired and cleaned up
JSON_SER_ERR = "<not serializable>"
MIME_TYPE_JSON = "application/json"
STATS_FIELD_LIST = ["not_found", "processed", "skipped"]
TXT_INVALID_TOKEN = "Invalid continuation token received. Either the token does not exist or the token has expired"
TXT_QUERY_PATH_REQUIRED = "A URL encoded path is required in the 'path' query parameter"


class SimpleCache:
    class RepeatTimer(threading.Timer):
        def run(self):
            while not self.finished.wait(self.interval):
                self.function(*self.args, **self.kwargs)

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
        self.timer = self.RepeatTimer(self.cache_clean_interval, self._clean_cache)
        self.timer.start()

    def __del__(self):
        self.timer.cancel()
        self._clean_cache(force=True)
        self.lock.release()

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
                "cache_size": self.cache_size,
                "cache_count": self.cache_count,
                "cache_overflow_count": self.cache_overflow_count,
                "cache_entries": len(self.cache.keys()),
                "cached_files": self.cached_files,
                "cached_memory": self.cached_memory,
            }
        )

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
        LOG.debug({"msg": "Cached item", "len": comp_data_len, "cache_size": self.cache_size})
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


def add_diskover_fields(file_info):
    return {
        "atime": datetime.datetime.fromtimestamp(file_info["atime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "ctime": datetime.datetime.fromtimestamp(file_info["ctime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "extension": file_info["file_ext"],
        "group": file_info["perms_unix_gid"],
        "ino": file_info["inode"],
        "mtime": datetime.datetime.fromtimestamp(file_info["mtime"]).strftime(DEFAULT_TIME_FORMAT_8601),
        "name": file_info["file_name"],
        "nlink": file_info["file_hard_links"],
        "owner": file_info["perms_unix_uid"],
        "parent_path": file_info["file_path"],
        "size": file_info["size"],
        "size_du": file_info["size_physical"],
        "type": "directory" if file_info["file_type"] == "dir" else file_info["file_type"],
        "pscale": file_info,
    }


def file_handler_pscale(root, filename_list, args={}):
    """Gets the metadata for the files/directories based at root and given the file/dir names in filename_list

    Parameters
    ----------
    root: <string> Root directory to start the scan
    filename_list: <list:string> List of file and directory names to retrieve metadata
    args: <dict> Dictionary containing parameters to control the scan
            {
              "custom_tagging": <bool>          # When true call a custom handler for each file
              "extra_attr": <bool>              # When true, gets extra OneFS metadata
              "no_acl": <bool>                  # When true, skip ACL parsing
              "phys_block_size": <int>          # Number of bytes in a block for the underlying storage device
              "nodepool_translation": <dict>    # Dictionary with a node pool number to text string translation
              "strip_dot_snapshot": <bool>      # When true, strip the .snapshot name from the file path returned
              "user_attr": <bool>               # When true, get user attribute data for files
            }

    Returns
    ----------
    dict - A dictionary representing the root and files scanned
            {
              "dirs": [<dict>]                  # List of directory metadata objects
              "files": [<dict>]                 # List of file metadata objects
              "stats": {
                "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
                "not_found": <int>              # Number of files that were not found
                "processed": <int>              # Number of files actually processed
                "skipped": <int>                # Number of files skipped
                "time_access_time": <int>       # Seconds spent getting the file access time
                "time_acl": <int>               # Seconds spent getting file ACL
                "time_custom_tagging": <int>    # Seconds spent processing custom tags
                "time_dinode": <int>            # Seconds spent getting OneFS metadata
                "time_extra_attr": <int>        # Seconds spent getting extra OneFS metadata
                "time_lstat": <int>             # Seconds spent in lstat
                "time_scan_dir": <int>          # Seconds spent scanning the entire directory
                "time_user_attr": <int>         # Seconds spent scanning user attributes
              }
            }
    """
    now = time.time()
    custom_tagging = args.get("custom_tagging", False)
    extra_attr = args.get("extra_attr", DEFAULT_PARSE_EXTRA_ATTR)
    no_acl = args.get("no_acl", DEFAULT_PARSE_SKIP_ACLS)
    phys_block_size = args.get("phys_block_size", IFS_BLOCK_SIZE)
    pool_translate = args.get("nodepool_translation", {})
    strip_dot_snapshot = args.get("strip_dot_snapshot", DEFAULT_STRIP_DOT_SNAPSHOT)
    user_attr = args.get("user_attr", DEFAULT_PARSE_USER_ATTR)

    result_list = []
    result_dir_list = []
    stats = {
        "lstat_required": 0,
        "not_found": 0,
        "processed": 0,
        "skipped": 0,
        "time_access_time": 0,
        "time_acl": 0,
        "time_custom_tagging": 0,
        "time_dinode": 0,
        "time_extra_attr": 0,
        "time_lstat": 0,
        "time_scan_dir": 0,
        "time_user_attr": 0,
    }

    for filename in filename_list:
        try:
            full_path = os.path.join(root, filename)
            fd = None
            try:
                fd = os.open(full_path, os.O_RDONLY | os.O_NOFOLLOW | os.O_OPENLINK)
            except FileNotFoundError:
                LOG.debug({"msg": "File not found", "file_path": full_path})
                stats["not_found"] += 1
                continue
            except Exception as e:
                if e.errno in (errno.ENOTSUP, errno.EACCES):  # 45: Not supported, 13: No access
                    stats["lstat_required"] += 1
                    LOG.debug({"msg": "Unable to call os.open. Using os.lstat instead", "file_path": full_path})
                    time_start = time.time()
                    file_info = get_file_stat_diskover(
                        root, filename, phys_block_size, strip_dot_snapshot=strip_dot_snapshot
                    )
                    stats["time_lstat"] += time.time() - time_start
                    if custom_tagging:
                        time_start = time.time()
                        file_info["user_tags"] = custom_tagging(file_info)
                        stats["time_custom_tagging"] += time.time() - time_start
                    if file_info["file_type"] == "dir":
                        file_info["_scan_time"] = now
                        result_dir_list.append(file_info)
                        # Fix size issues with dirs
                        file_info["size_logical"] = 0
                        stats["processed"] += 1
                        continue
                    result_list.append(file_info)
                    stats["processed"] += 1
                    continue
                LOG.exception({"msg": "Error found when calling os.open", "file_path": full_path, "error": str(e)})
                continue
            time_start = time.time()
            fstats = attr.get_dinode(fd)
            stats["time_dinode"] += time.time() - time_start
            # atime call can return empty if the file does not have an atime or atime tracking is disabled
            time_start = time.time()
            atime = attr.get_access_time(fd)
            stats["time_access_time"] += time.time() - time_start
            if atime:
                atime = atime[0]
            else:
                # If atime does not exist, use the last metadata change time as this captures the last time someone
                # modified either the data or the inode of the file
                atime = fstats["di_ctime"]
            di_data_blocks = fstats.get("di_data_blocks", fstats["di_physical_blocks"] - fstats["di_protection_blocks"])
            logical_blocks = fstats["di_logical_size"] // phys_block_size
            comp_blocks = logical_blocks - fstats["di_shadow_refs"]
            compressed_file = True if (di_data_blocks and comp_blocks) else False
            stubbed_file = (fstats["di_flags"] & IFLAG_COMBO_STUBBED) > 0
            if strip_dot_snapshot:
                file_path = re.sub(RE_STRIP_SNAPSHOT, "", root, count=1)
            else:
                file_path = root
            file_info = {
                # ========== Timestamps ==========
                "atime": atime,
                "atime_date": datetime.date.fromtimestamp(atime).isoformat(),
                "btime": fstats["di_create_time"],
                "btime_date": datetime.date.fromtimestamp(fstats["di_create_time"]).isoformat(),
                "ctime": fstats["di_ctime"],
                "ctime_date": datetime.date.fromtimestamp(fstats["di_ctime"]).isoformat(),
                "mtime": fstats["di_mtime"],
                "mtime_date": datetime.date.fromtimestamp(fstats["di_mtime"]).isoformat(),
                # ========== File and path strings ==========
                "file_path": file_path,
                "file_name": filename,
                "file_ext": os.path.splitext(filename)[1],
                # ========== File attributes ==========
                "file_access_pattern": ACCESS_PATTERN[fstats["di_la_pattern"]],
                "file_compression_ratio": comp_blocks / di_data_blocks if compressed_file else 1,
                "file_hard_links": fstats["di_nlink"],
                "file_is_ads": ((fstats["di_flags"] & IFLAGS_UF_HASADS) != 0),
                "file_is_compressed": (comp_blocks > di_data_blocks) if compressed_file else False,
                "file_is_dedupe_disabled": not not fstats["di_no_dedupe"],
                "file_is_deduped": (fstats["di_shadow_refs"] > 0),
                "file_is_inlined": (
                    (fstats["di_physical_blocks"] == 0)
                    and (fstats["di_shadow_refs"] == 0)
                    and (fstats["di_logical_size"] > 0)
                ),
                "file_is_packed": not not fstats["di_packing_policy"],
                "file_is_smartlinked": stubbed_file,
                "file_is_sparse": ((fstats["di_logical_size"] < fstats["di_size"]) and not stubbed_file),
                "file_type": FILE_TYPE[fstats["di_mode"] & FILE_TYPE_MASK],
                "inode": fstats["di_lin"],
                "inode_mirror_count": fstats["di_inode_mc"],
                "inode_parent": fstats["di_parent_lin"],
                "inode_revision": fstats["di_rev"],
                # ========== Storage pool targets ==========
                "pool_target_data": fstats["di_data_pool_target"],
                "pool_target_data_name": pool_translate.get(
                    int(fstats["di_data_pool_target"]), str(fstats["di_data_pool_target"])
                ),
                "pool_target_metadata": fstats["di_metadata_pool_target"],
                "pool_target_metadata_name": pool_translate.get(
                    int(fstats["di_metadata_pool_target"]), str(fstats["di_metadata_pool_target"])
                ),
                # ========== Permissions ==========
                "perms_unix_bitmask": stat.S_IMODE(fstats["di_mode"]),
                "perms_unix_gid": fstats["di_gid"],
                "perms_unix_uid": fstats["di_uid"],
                # ========== File protection level ==========
                "protection_current": fstats["di_current_protection"],
                "protection_target": fstats["di_protection_policy"],
                # ========== File allocation size and blocks ==========
                # The apparent size of the file. Sparse files include the sparse area
                "size": fstats["di_size"],
                # Logical size in 8K blocks. Sparse files only show the real data portion
                "size_logical": fstats["di_logical_size"],
                # Physical size on disk including protection overhead, including extension blocks and excluding metadata
                "size_physical": fstats["di_physical_blocks"] * phys_block_size,
                # Physical size on disk excluding protection overhead and excluding metadata
                "size_physical_data": di_data_blocks * phys_block_size,
                # Physical size on disk of the protection overhead
                "size_protection": fstats["di_protection_blocks"] * phys_block_size,
                # ========== SSD usage ==========
                "ssd_strategy": fstats["di_la_ssd_strategy"],
                "ssd_strategy_name": SSD_STRATEGY[fstats["di_la_ssd_strategy"]],
                "ssd_status": fstats["di_la_ssd_status"],
                "ssd_status_name": SSD_STATUS[fstats["di_la_ssd_status"]],
            }
            if not no_acl:
                time_start = time.time()
                acl = onefs_acl.get_acl_dict(fd)
                stats["time_acl"] += time.time() - time_start
                file_info["perms_acl_aces"] = misc.ace_list_to_str_list(acl.get("aces"))
                file_info["perms_acl_group"] = misc.acl_group_to_str(acl)
                file_info["perms_acl_user"] = misc.acl_user_to_str(acl)
            if extra_attr:
                # di_flags may have other bits we need to translate
                #     Coalescer setting (on|off|endurant all|coalescer only)
                #     IFLAGS_UF_WRITECACHE and IFLAGS_UF_WC_ENDURANT flags
                # Do we want inode locations? how many on SSD and spinning disk?
                #   - Get data from estats["ge_iaddrs"], e.g. ge_iaddrs: [(1, 13, 1098752, 512)]
                # Extended attributes/custom attributes?
                time_start = time.time()
                estats = attr.get_expattr(fd)
                stats["time_extra_attr"] += time.time() - time_start
                # Add up all the inode sizes
                metadata_size = 0
                for inode in estats["ge_iaddrs"]:
                    metadata_size += inode[3]
                # Sum of the size of all the inodes. This includes inodes that mix both 512 byte and 8192 byte inodes
                file_info["size_metadata"] = metadata_size
                file_info["file_is_manual_access"] = not not estats["ge_manually_manage_access"]
                file_info["file_is_manual_packing"] = not not estats["ge_manually_manage_packing"]
                file_info["file_is_manual_protection"] = not not estats["ge_manually_manage_protection"]
                if estats["ge_coalescing_ec"] & estats["ge_coalescing_on"]:
                    file_info["file_coalescer"] = "coalescer on, ec off"
                elif estats["ge_coalescing_on"]:
                    file_info["file_coalescer"] = "coalescer on, ec on"
                elif estats["ge_coalescing_ec"]:
                    file_info["file_coalescer"] = "coalescer off, ec on"
                else:
                    file_info["file_coalescer"] = "coalescer off, ec off"
            if user_attr:
                extended_attr = {}
                time_start = time.time()
                keys = uattr.userattr_list(fd)
                for key in keys:
                    extended_attr[key] = uattr.userattr_get(fd, key)
                stats["time_user_attr"] += time.time() - time_start
                file_info["user_attributes"] = extended_attr
            if custom_tagging:
                time_start = time.time()
                file_info["user_tags"] = custom_tagging(file_info)
                stats["time_custom_tagging"] += time.time() - time_start

            time_start = time.time()
            lstat_required = translate_user_group_perms(full_path, file_info)
            if lstat_required:
                stats["lstat_required"] += 1
                stats["time_lstat"] += time.time() - time_start

            if fstats["di_mode"] & 0o040000:
                file_info["_scan_time"] = now
                result_dir_list.append(file_info)
                # Fix size issues with dirs
                file_info["size_logical"] = 0
                stats["processed"] += 1
                continue
            result_list.append(file_info)
            if (
                (fstats["di_mode"] & 0o010000 == 0o010000)
                or (fstats["di_mode"] & 0o120000 == 0o120000)
                or (fstats["di_mode"] & 0o140000 == 0o140000)
            ):
                # Fix size issues with symlinks, sockets, and FIFOs
                file_info["size_logical"] = 0
            stats["processed"] += 1
        except IOError as ioe:
            stats["skipped"] += 1
            if ioe.errno == errno.EACCES:  # 13: No access
                LOG.warn({"msg": "Permission error", "file_path": full_path})
            else:
                LOG.exception(ioe)
        except FileNotFoundError as fnfe:
            stats["not_found"] += 1
            LOG.warn({"msg": "File not found", "file_path": full_path})
        except PermissionError as pe:
            stats["skipped"] += 1
            LOG.warn({"msg": "Permission error", "file_path": full_path})
            LOG.exception(pe)
        except Exception as e:
            stats["skipped"] += 1
            LOG.exception(e)
        finally:
            try:
                os.close(fd)
            except:
                pass
    stats["time_scan_dir"] = time.time() - now
    results = {
        "dirs": result_dir_list,
        "files": result_list,
        "stats": stats,
    }
    return results


def file_handler_pscale_diskover(root, filename_list, args={}):
    scan_results = file_handler_pscale(root, filename_list, args)
    now = time.time()
    dirs_list = scan_results["dirs"]
    files_list = scan_results["files"]
    for i in range(len(dirs_list)):
        dirs_list[i] = add_diskover_fields(dirs_list[i])
    for i in range(len(files_list)):
        files_list[i] = add_diskover_fields(files_list[i])
    scan_results["stats"]["time_conversion"] = time.time() - now
    return scan_results


def get_directory_listing(path):
    try:
        if use_scandir:
            dir_file_list = []
            for entry in os.scandir(path):
                dir_file_list.append(entry.name)
        else:
            dir_file_list = os.listdir(path)
    except IOError as ioe:
        dir_file_list = []
        if ioe.errno == 13:
            LOG.debug({"msg": "Directory permission error", "path": path, "error": str(ioe)})
        else:
            LOG.debug({"msg": "Unknown error", "path": path, "error": str(ioe)})
    except PermissionError as pe:
        dir_file_list = []
        LOG.debug({"msg": "Directory permission error", "path": path, "error": str(pe)})
    except Exception as e:
        LOG.debug({"msg": "Unknown error", "path": path, "error": str(e)})
    return dir_file_list


def parse_arg_bool(arg, field, default):
    val = arg.get(field, default)
    if isinstance(val, bool):
        return val
    val = val.lower()
    if val in ["false", "0"]:
        return False
    return True


def parse_arg_int(arg, field, default, minimum=None, maximum=None):
    val = int(arg.get(field, default))
    if minimum and val < minimum:
        val = minimum
    if maximum and val > maximum:
        val = maximum
    return val


def get_path_from_urlencoded(urlencoded_path):
    decoded_path = urlunquote(urlencoded_path)
    if decoded_path.endswith("/"):
        decoded_path = decoded_path[0:-1]
    if not decoded_path.startswith("/"):
        decoded_path = "/" + decoded_path
    if not decoded_path.startswith("/ifs"):
        decoded_path = "/ifs" + decoded_path
    root = os.path.dirname(decoded_path)
    file = os.path.basename(decoded_path)
    return root, file, decoded_path


def translate_user_group_perms(full_path, file_info):
    lstat_required = False
    # Populate the perms_user and perms_group fields from the avaialble SID and UID/GID data
    # Translate the numeric values into human readable user name and group names if possible
    # TODO: Add translation to names from SID/UID/GID values
    if file_info["perms_unix_uid"] == 0xFFFFFFFF or file_info["perms_unix_gid"] == 0xFFFFFFFF:
        lstat_required = True
        LOG.debug({"msg": "lstat required for UID/GID", "file_path": full_path})
        # If the UID/GID is set to 0xFFFFFFFF then on cluster, the UID/GID is generated
        # by the cluster.
        # When this happens, use os.fstat to get the UID/GID information from the point
        # of view of the access zone that is running the script, normally the System zone
        try:
            fstats = os.lstat(full_path)
            file_info["perms_unix_gid"] = (fstats.st_gid,)
            file_info["perms_unix_uid"] = (fstats.st_uid,)
        except Exception as e:
            LOG.info({"msg": "Unable to get file UID/GID", "file_path": full_path})
    if "perms_acl_user" in file_info:
        file_info["perms_user"] = file_info["perms_acl_user"]
        file_info["perms_user"].replace("uid:", "")
    else:
        file_info["perms_user"] = file_info["perms_unix_uid"]
    if "perms_acl_group" in file_info:
        file_info["perms_group"] = file_info["perms_acl_group"]
        file_info["perms_group"].replace("gid:", "")
    else:
        file_info["perms_group"] = file_info["perms_unix_gid"]
    return lstat_required


@app.after_request
def compress(response):
    accept_encoding = request.headers.get("accept-encoding", "").lower()
    if (
        response.status_code < 200
        or response.status_code >= 300
        or response.direct_passthrough
        or "gzip" not in accept_encoding
        or "Content-Encoding" in response.headers
    ):
        return response
    # 0: No compression, 1: Fastest, 9: Slowest
    content = gzip.zlib.compress(response.get_data(), 9)
    response.set_data(content)
    response.headers["Content-Length"] = len(content)
    response.headers["Content-Encoding"] = "gzip"
    return response


@app.route("/cluster_storage_stats", methods=["GET"])
def handle_cluster_storage_stats():
    args = request.args
    storage_usage_stats = {}
    if misc.is_onefs_os():
        storage_usage_stats = misc.get_local_storage_usage_stats()
    else:
        # TODO: Add support for querying for usage statistics
        pass
    resp = Response(json.dumps(storage_usage_stats), mimetype=MIME_TYPE_JSON)
    return resp


@app.route("/ps_stat/list", methods=["GET"])
def handle_ps_stat_list():
    """Returns metadata for both the root path and all the immediate children of that path
    In the case the root is a file, only the file metadata will be returned

    Query arguments (common)
    ----------
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    limit: <int> Maximum number of entries to return in a single call. Defaults to 10000. Maximum value of 100000
    token: <string> Token string to allow the continuation of a previous scan request when that request did not return
            all the available data for a specific root path. Tokens expire and using an expired token results in a 404
    type: <string> Type of scan result to return. One of: powerscale|diskover. The default is powerscale.

    Query arguments (optional)
    ----------
    custom_tagging: <bool> When true call a custom handler for each file. Enabling this can slow down scan speed
    extra_attr: <bool> When true, gets extra OneFS metadata. Enabling this can slow down scan speed
    no_acl: <bool> When true, skip ACL parsing. Enabling this can speed up scanning but results will not have ACLs
    strip_dot_snapshot: <bool> When true, strip the .snapshot name from the file path returned
    user_attr: <bool> # When true, get user attribute data for files. Enabling this can slow down scan speed

    Returns
    ----------
    dict - A dictionary representing the root and files scanned
        {
          "contents": {
            "dirs": [<dict>]                  # List of directory metadata objects
            "files": [<dict>]                 # List of file metadata objects
            "root": <dict>                    # Metadata for the root path
            "stats": {
              "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
              "not_found": <int>              # Number of files that were not found
              "processed": <int>              # Number of files actually processed
              "skipped": <int>                # Number of files skipped
              "time_access_time": <int>       # Seconds spent getting the file access time
              "time_acl": <int>               # Seconds spent getting file ACL
              "time_custom_tagging": <int>    # Seconds spent processing custom tags
              "time_dinode": <int>            # Seconds spent getting OneFS metadata
              "time_extra_attr": <int>        # Seconds spent getting extra OneFS metadata
              "time_lstat": <int>             # Seconds spent in lstat
              "time_scan_dir": <int>          # Seconds spent scanning the entire directory
              "time_user_attr": <int>         # Seconds spent scanning user attributes
            }
          }
          "items_total": <int>                # Total number of items remaining that could be returned
          "items_returned": <int>             # Number of metadata items returned. This number includes the "root"
          "token_continuation": <string>      # String that should be used in the "token" query argument to continue
                                              # scanning a directory
          "token_expiration": <int>           # Epoch seconds specifying when the token will expire
          "stats": {
            "lstat_required": <bool>          # Number of times lstat was called vs. internal stat call
            "not_found": <int>                # Number of files that were not found
            "processed": <int>                # Number of files actually processed
            "skipped": <int>                  # Number of files skipped
            "time_access_time": <int>         # Seconds spent getting the file access time
            "time_acl": <int>                 # Seconds spent getting file ACL
            "time_custom_tagging": <int>      # Seconds spent processing custom tags
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }
    """
    args = request.args
    param = {
        "custom_tagging": parse_arg_bool(args, "custom_tagging", False),
        "extra_attr": parse_arg_bool(args, "extra_attr", False),
        "limit": parse_arg_int(args, "limit", DEFAULT_ITEM_LIMIT, 1, DEFAULT_MAX_ITEM_LIMIT),
        "no_acl": parse_arg_bool(args, "no_acl", False),
        "nodepool_translation": app.config["ps_scan"]["nodepool_translation"],
        "path": args.get("path"),
        "strip_do_snapshot": parse_arg_bool(args, "strip_dot_snapshot", True),
        "token": args.get("token"),
        "type": args.get("type", DEFAULT_DATA_TYPE),
        "user_attr": parse_arg_bool(args, "user_attr", False),
    }
    if not param["path"]:
        return make_response({"msg": TXT_QUERY_PATH_REQUIRED}, 404)

    dir_handler = file_handler_pscale if param["type"] == DATA_TYPE_PS else file_handler_pscale_diskover
    dir_list = []
    dir_list_len = 0
    list_stat_data = {}
    root = {}
    root_is_dir = False
    stat_data = None
    token_continuation = ""
    token_expiration = ""

    # Get the base directory, the last path component, and the full path. e.g. /ifs, foo, /ifs/foo
    base, file, full_path = get_path_from_urlencoded(param["path"])
    if not param["token"]:
        stat_data = dir_handler(base, [file], param)
        if stat_data["dirs"]:
            root = stat_data["dirs"][0]
            root_is_dir = True
            param["limit"] -= 1
        elif stat_data["files"]:
            root = stat_data["files"][0]
            param["limit"] -= 1

    if root_is_dir or param["token"]:
        # Get the list of files/directories to process either from the file system directly or a cache
        if param["token"]:
            # Get the cached list and then set dir_list
            LOG.debug({"msg": "Getting cached directory listing", "path": full_path})
            cached_item = app.config["cache"].get_item(param["token"])
            if not cached_item:
                return make_response({"msg": TXT_INVALID_TOKEN, "token": param["token"]}, 404)
            base = cached_item["base"]
            dir_list = cached_item["dir_list"]
            offset = cached_item["offset"] + param["limit"]
        else:
            # Process the direct children of the passed in path
            LOG.debug({"msg": "Getting directory listing", "path": full_path})
            dir_list = get_directory_listing(full_path)
            offset = param["limit"]
        # Split this list up into chunks dependent on the 'limit' query value
        dir_list_len = len(dir_list)
        if dir_list_len > param["limit"]:
            # Cache the remainder of the directory listing to avoid re-scanning the directory
            token_data = app.config["cache"].add_item(
                {"base": full_path, "dir_list": dir_list[param["limit"]:], "offset": offset}
            )
            dir_list = dir_list[0:param["limit"]]
            token_continuation = token_data["token"]
            token_expiration = token_data["expiration"]
        # Perform the actual stat commands on each file/directory
        if dir_list:
            list_stat_data = dir_handler(full_path, dir_list, param)

    # Calculate statistics to return in the response
    dirs_len = len(list_stat_data.get("dirs", []))
    files_len = len(list_stat_data.get("files", []))
    items_total = dir_list_len + 1 * (not not root)
    items_returned = dirs_len + files_len + 1 * (not not root)
    total_stats = {}
    if stat_data:
        if list_stat_data:
            for key in list_stat_data["stats"]:
                total_stats[key] = list_stat_data["stats"][key] + stat_data["stats"][key]
        else:
            total_stats = stat_data["stats"]
    else:
        total_stats = list_stat_data["stats"]

    # Build response
    resp_data = {
        "contents": {
            "dirs": list_stat_data.get("dirs", []),
            "files": list_stat_data.get("files", []),
            "root": root,
        },
        "items_total": items_total,
        "items_returned": items_returned,
        "token_continuation": token_continuation,
        "token_expiration": token_expiration,
        "stats": total_stats,
    }
    return Response(json.dumps(resp_data, default=lambda o: JSON_SER_ERR), mimetype=MIME_TYPE_JSON)


@app.route("/ps_stat/single", methods=["GET"])
def handle_ps_stat_single():
    """Returns metadata for a single file or directory specified by the "path" argument

    Query arguments (common)
    ----------
    path: <string> URL encoded string representing the path in the file system that metadata will be returned
            The path can start with /ifs or not. The /ifs part of the path will be prepended if necessary
            A path with a trailing slash will have the slash removed.
    type: <string> Type of scan result to return. One of: powerscale|diskover. The default is powerscale.

    Query arguments (optional)
    ----------
    custom_tagging: <bool> When true call a custom handler for each file. Enabling this can slow down scan speed
    extra_attr: <bool> When true, gets extra OneFS metadata. Enabling this can slow down scan speed
    no_acl: <bool> When true, skip ACL parsing. Enabling this can speed up scanning but results will not have ACLs
    strip_dot_snapshot: <bool> When true, strip the .snapshot name from the file path returned
    user_attr: <bool> # When true, get user attribute data for files. Enabling this can slow down scan speed

    Returns
    ----------
    dict - A dictionary representing the single item scanned
        {
          "contents": {
            "dirs": []                        # Empty list
            "files": []                       # Empty list
            "root": <dict>                    # Metadata for the root path
            "stats": {
              "lstat_required": <bool>        # Number of times lstat was called vs. internal stat call
              "not_found": <int>              # Number of files that were not found
              "processed": <int>              # Number of files actually processed
              "skipped": <int>                # Number of files skipped
              "time_access_time": <int>       # Seconds spent getting the file access time
              "time_acl": <int>               # Seconds spent getting file ACL
              "time_custom_tagging": <int>    # Seconds spent processing custom tags
              "time_dinode": <int>            # Seconds spent getting OneFS metadata
              "time_extra_attr": <int>        # Seconds spent getting extra OneFS metadata
              "time_lstat": <int>             # Seconds spent in lstat
              "time_scan_dir": <int>          # Seconds spent scanning the entire directory
              "time_user_attr": <int>         # Seconds spent scanning user attributes
            }
          }
          "items_total": <int>                # Total number of items remaining that could be returned
          "items_returned": <int>             # Number of metadata items returned. This number includes the "root"
          "stats": {
            "lstat_required": <bool>          # Number of times lstat was called vs. internal stat call
            "not_found": <int>                # Number of files that were not found
            "processed": <int>                # Number of files actually processed
            "skipped": <int>                  # Number of files skipped
            "time_access_time": <int>         # Seconds spent getting the file access time
            "time_acl": <int>                 # Seconds spent getting file ACL
            "time_custom_tagging": <int>      # Seconds spent processing custom tags
            "time_dinode": <int>              # Seconds spent getting OneFS metadata
            "time_extra_attr": <int>          # Seconds spent getting extra OneFS metadata
            "time_lstat": <int>               # Seconds spent in lstat
            "time_scan_dir": <int>            # Seconds spent scanning the entire directory
            "time_user_attr": <int>           # Seconds spent scanning user attributes
          }
        }
    """
    args = request.args
    param = {
        "custom_tagging": parse_arg_bool(args, "custom_tagging", False),
        "extra_attr": parse_arg_bool(args, "extra_attr", False),
        "no_acl": parse_arg_bool(args, "no_acl", False),
        "nodepool_translation": app.config["ps_scan"]["nodepool_translation"],
        "path": args.get("path"),
        "strip_do_snapshot": parse_arg_bool(args, "strip_dot_snapshot", True),
        "type": args.get("type", DEFAULT_DATA_TYPE),
        "user_attr": parse_arg_bool(args, "user_attr", False),
    }

    base, file, full = get_path_from_urlencoded(param["path"])
    if param["type"] == DATA_TYPE_PS:
        stat_data = file_handler_pscale(base, [file], param)
    else:
        stat_data = file_handler_pscale_diskover(base, [file], param)
    if stat_data["dirs"]:
        root = stat_data["dirs"][0]
    elif stat_data["files"]:
        root = stat_data["files"][0]
    else:
        root = {}
    resp_data = {
        "contents": {
            "dirs": [],
            "files": [],
            "root": root,
        },
        "items_total": 1,
        "items_returned": 1 * (not not root),
        "stats": stat_data["stats"],
    }
    return Response(json.dumps(resp_data, default=lambda o: JSON_SER_ERR), mimetype=MIME_TYPE_JSON)


if __name__ == "__main__" or __file__ == None:
    # DEBUG: Add CLI parsing and populate the app.config["ps_scan"] with config variables
    app.config["cache"] = SimpleCache()
    app.config["ps_scan"] = {"nodepool_translation": misc.get_nodepool_translation()}
    serve(app, listen="*:4242")
