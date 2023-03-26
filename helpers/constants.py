#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "constants"
__version__       = "1.0.0"
__date__          = "20 March 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"


ACCESS_PATTERN = [
    "concurrency",
    "streaming",
    "random",
    "disabled",
    "invalid",
]
FILE_TYPE = {
    0o010000: "fifo",
    0o020000: "char",
    0o040000: "dir",
    0o060000: "block",
    0o100000: "file",
    0o120000: "symlink",
    0o140000: "socket",
}
FILE_TYPE_MASK = 0o170000
IFS_BLOCK_SIZE = 8192
PS_SCAN_MAPPING = {
    "properties": {
        # ========== Timestamps ==========
        "atime": {"type": "long"},
        "atime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "btime": {"type": "long"},
        "btime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "ctime": {"type": "long"},
        "ctime_date": {"type": "date", "format": "yyyy-MM-dd"},
        "mtime": {"type": "long"},
        "mtime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # ========== File and path strings ==========
        "root": {"type": "keyword"},
        "filename": {"type": "keyword"},
        "extension": {"type": "keyword"},
        # ========== File attributes ==========
        "access_pattern": {"type": "keyword"},
        "file_compressed": {"type": "boolean"},
        "file_compression_ratio": {"type": "float"},
        "file_dedupe_disabled": {"type": "boolean"},
        "file_deduped": {"type": "boolean"},
        "file_has_ads": {"type": "boolean"},
        "file_inlined": {"type": "boolean"},
        "file_packed": {"type": "boolean"},
        "file_smartlinked": {"type": "boolean"},
        "file_sparse": {"type": "boolean"},
        "file_type": {"type": "keyword"},
        "hard_links": {"type": "short"},
        "inode": {"type": "long"},
        "inode_mirror_count": {"type": "byte"},
        "inode_parent": {"type": "long"},
        "inode_revision": {"type": "long"},
        # ========== Storage pool targets ==========
        "pool_target_data": {"type": "keyword"},
        "pool_target_data_name": {"type": "keyword"},
        "pool_target_metadata": {"type": "keyword"},
        "pool_target_metadata_name": {"type": "keyword"},
        # ========== Permissions ==========
        "perms_gid": {"type": "long"},
        "perms_uid": {"type": "long"},
        "perms_unix": {"type": "short"},
        # ========== File protection level ==========
        "protection_current": {"type": "keyword"},
        "protection_target": {"type": "keyword"},
        # ========== File allocation size and blocks ==========
        "size": {"type": "long"},
        "size_logical": {"type": "long"},
        "size_physical": {"type": "long"},
        "size_physical_data": {"type": "long"},
        "size_protection": {"type": "long"},
        # ========== SSD usage ==========
        "ssd_strategy": {"type": "short"},
        "ssd_strategy_name": {"type": "keyword"},
        "ssd_status": {"type": "short"},
        "ssd_status_name": {"type": "keyword"},
        "size_metadata": {"type": "integer"},
        "manual_access": {"type": "boolean"},
        "manual_packing": {"type": "boolean"},
        "manual_protection": {"type": "boolean"},
        # ========== User attributes ==========
        "tags": {"type": "keyword"},
        "user_attributes": {"type": "object"},
    }
}
SSD_STRATEGY = [
    "metadata read",
    "avoid ssd",
    "data on ssd",
    "metadata write",
    "invalid",
]
SSD_STATUS = [
    "metadata restripe",
    "data restripe",
    "restriping",
    "complete",
    "invalid",
]
# Inode flag bit fields
# fmt: off
IFLAGS_UF_NODUMP        = 0x00000001 # do not dump file
IFLAGS_UF_IMMUTABLE     = 0x00000002 # file may not be changed
IFLAGS_UF_APPEND        = 0x00000004 # writes to file may only append
IFLAGS_UF_OPAQUE        = 0x00000008 # directory is opaque wrt. union
IFLAGS_UF_NOUNLINK      = 0x00000010 # file may not be removed or renamed
IFLAGS_UF_INHERIT       = 0x00000020 # unused but set on all files
IFLAGS_UF_WRITECACHE    = 0x00000040 # writes are cached (Is this reverse bit? 0 is enable 1 is disable?)
IFLAGS_UF_WC_INHERIT    = 0x00000080 # unused but set on all new files
IFLAGS_UF_DOS_NOINDEX   = 0x00000100
IFLAGS_UF_ADS           = 0x00000200 # file is ADS directory or stream
IFLAGS_UF_HASADS        = 0x00000400 # file has ADS dir
IFLAGS_UF_WC_ENDURANT   = 0x00000800 # write cache is endurant  (Is this reverse bit? 0 is enable 1 is disable?)
IFLAGS_UF_SPARSE        = 0x00001000
IFLAGS_UF_REPARSE       = 0x00002000
IFLAGS_UF_ISI_UNUSED1   = 0x00004000
IFLAGS_UF_DOS_OFFLINE   = 0x00008000
IFLAGS_UF_DOS_ARCHIVE   = 0x10000000
IFLAGS_UF_DOS_HIDDEN    = 0x20000000
IFLAGS_UF_DOS_RO        = 0x40000000
IFLAGS_UF_DOS_SYSTEM    = 0x80000000
# Super user changeable flags
IFLAGS_SF_ARCHIVED      = 0x00010000
IFLAGS_SF_IMMUTABLE     = 0x00020000
IFLAGS_SF_APPEND_ONLY   = 0x00040000
IFLAGS_SF_STUBBED       = 0x00080000
IFLAGS_SF_NO_UNLINK     = 0x00100000
IFLAGS_SF_SNAP_INODE    = 0x00200000
IFLAGS_SF_NO_SNAP_INODE = 0x00400000
IFLAGS_SF_STUBBED_CACHE = 0x00800000
IFLAGS_SF_HAS_NTFS_ACL  = 0x01000000
IFLAGS_SF_HAS_NTFS_OG   = 0x02000000
IFLAGS_SF_PARENT_UPGD   = 0x04000000
IFLAGS_SF_BACKUP_SPARSE = 0x08000000
# Combination flags
IFLAG_COMBO_UF_DOS_ATTR = IFLAGS_UF_DOS_OFFLINE | IFLAGS_UF_DOS_ARCHIVE | IFLAGS_UF_DOS_HIDDEN | IFLAGS_UF_DOS_RO | IFLAGS_UF_DOS_SYSTEM
IFLAG_COMBO_STUBBED     = IFLAGS_SF_STUBBED | IFLAGS_SF_STUBBED_CACHE
# fmt: on
"""
Unused dinode fields:
  di_at_r_repair
  di_at_r_repairing
  di_attr_bytes
  di_bad_sin
  di_bug_124996
  di_create_timensec
  di_ctimensec
  di_data_bytes
  di_data_depth
  di_data_fmt
  di_dir_version
  di_in_cstat
  di_inode_version
  di_istrashdir
  di_la_drivecount
  di_magic
  di_max_pg_n
  di_mtimensec
  di_packing_incomplete
  di_packing_target
  di_parent_hash
  di_recovered_flag
  di_restripe_state
  di_stream # Normal is 0, 2 is usually for CloudPools targeting the writes to local cache
  di_upgrade_async
  di_upgrading
"""

# fmt: off
CMD_EXIT            = 0x0001
CMD_SEND            = 0x0002
CMD_SEND_DIR        = 0x0004
CMD_SEND_STATS      = 0x0008
CMD_SEND_DIR_COUNT  = 0x0010
CMD_STATUS_IDLE     = 0x0100
CMD_STATUS_RUN      = 0x0200
CMD_REQ_DIR_COUNT   = 0x1000
CMD_REQ_DIR         = 0x2000
# fmt: on
DEFAULT_ES_THREADS = 4
DEFAULT_LOW_DIR_Q_THRESHOLD = 5
DEFAULT_MAX_Q_SIZE = 2000
DEFAULT_MAX_Q_WAIT_LOOPS = 100
DEFAULT_SEND_Q_SLEEP = 0.5
DEFAULT_STATS_OUTPUT_INTERVAL = 30
DEFAULT_THREAD_COUNT = 6
DEFAULT_THREADS_PER_PROC_COUNT = 4
