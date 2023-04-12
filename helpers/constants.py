#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "constants"
__version__       = "1.0.0"
__date__          = "10 April 2023"
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
        # Last access time of the file both in fractional seconds and YYYY-mm-DD format
        "atime": {"type": "long"},
        "atime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Birth/creation time of the file both in fractional seconds and YYYY-mm-DD format
        "btime": {"type": "long"},
        "btime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Last metadata change time of the file both in fractional seconds and YYYY-mm-DD format
        "ctime": {"type": "long"},
        "ctime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # Last data modified time of the file both in fractional seconds and YYYY-mm-DD format
        "mtime": {"type": "long"},
        "mtime_date": {"type": "date", "format": "yyyy-MM-dd"},
        # ========== File and path strings ==========
        # Parent directory of the file
        "file_path": {"type": "keyword"},
        # Full file name of the file including the extension but without the path
        "file_name": {"type": "keyword"},
        # File name extension portion. This is generally the text after the last . in the file name
        "file_ext": {"type": "keyword"},
        # ========== File attributes ==========
        "file_access_pattern": {"type": "keyword"},
        "file_compression_ratio": {"type": "float"},
        # Number of hard links for the file. Files start with 1. A number > 1 indicates other links to the file
        "file_hard_links": {"type": "short"},
        # does the file contain any alternative data streams
        "file_is_ads": {"type": "boolean"},
        # is the file compressed
        "file_is_compressed": {"type": "boolean"},
        # is the file file dedupe disabled - (0: can dedupe, 1: do not dedupe)
        "file_is_dedupe_disabled": {"type": "boolean"},
        # is the file deduped, assume the file is fully/partially deduped if it has shadow store references
        "file_is_deduped": {"type": "boolean"},
        # is the file data stored int the inode
        "file_is_inlined": {"type": "boolean"},
        "file_is_manual_access": {"type": "boolean"},
        "file_is_manual_packing": {"type": "boolean"},
        "file_is_manual_protection": {"type": "boolean"},
        # is the file packed into a container (SFSE or Small File Storage Efficiency)
        "file_is_packed": {"type": "boolean"},
        # is the file a SmartLink or a stub file for CloudPools
        "file_is_smartlinked": {"type": "boolean"},
        # is the file a sparse file
        "file_is_sparse": {"type": "boolean"},
        # Type of the file object, typically "file", "dir", or "symlink". Values defined by the FILE_TYPE constant
        "file_type": {"type": "keyword"},
        # inode value of the file
        "inode": {"type": "long"},
        # Number of inodes this file has
        "inode_mirror_count": {"type": "byte"},
        # The inode number of the parent directory
        "inode_parent": {"type": "long"},
        # Number of times the inode has been modified. An indicator of file change rate. Starts at 2
        "inode_revision": {"type": "long"},
        # ========== Storage pool targets ==========
        "pool_target_data": {"type": "keyword"},
        "pool_target_data_name": {"type": "keyword"},
        "pool_target_metadata": {"type": "keyword"},
        "pool_target_metadata_name": {"type": "keyword"},
        # ========== Permissions ==========
        "perms_acl_aces": {"type": "keyword"},
        "perms_acl_group": {"type": "keyword"},
        "perms_acl_user": {"type": "keyword"},
        "perms_gid": {"type": "long"},
        "perms_uid": {"type": "long"},
        "perms_unix": {"type": "short"},
        # ========== File protection level ==========
        "protection_current": {"type": "keyword"},
        "protection_target": {"type": "keyword"},
        # ========== File allocation size and blocks ==========
        "size": {"type": "long"},
        "size_logical": {"type": "long"},
        "size_metadata": {"type": "integer"},
        "size_physical": {"type": "long"},
        "size_physical_data": {"type": "long"},
        "size_protection": {"type": "long"},
        # ========== SSD usage ==========
        "ssd_strategy": {"type": "short"},
        "ssd_strategy_name": {"type": "keyword"},
        "ssd_status": {"type": "short"},
        "ssd_status_name": {"type": "keyword"},
        # ========== User attributes ==========
        "user_attributes": {"type": "object"},
        "user_tags": {"type": "keyword"},
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

# fmt: off
CMD_EXIT            = 0x0001
CMD_SEND            = 0x0002
CMD_SEND_DIR        = 0x0004
CMD_SEND_STATS      = 0x0008
CMD_SEND_DIR_COUNT  = 0x0010
CMD_SEND_FILE_COUNT = 0x0020
CMD_STATUS_IDLE     = 0x0100
CMD_STATUS_RUN      = 0x0200
CMD_REQ_DIR_COUNT   = 0x1000
CMD_REQ_FILE_COUNT  = 0x2000
CMD_REQ_DIR         = 0x4000
# fmt: on
DEFAULT_CMD_POLL_INTERVAL = 0.01
DEFAULT_DIR_OUTPUT_INTERVAL = 2
DEFAULT_DIR_REQUEST_INTERVAL = 2
DEFAULT_DIRQ_REQUEST_PERCENTAGE = 0.5
DEFAULT_ES_BULK_REFRESH_INTERVAL = "10m"
DEFAULT_ES_MAX_Q_SIZE = 2000
DEFAULT_ES_SEND_Q_SLEEP = 0.5
DEFAULT_ES_THREADS = 4
DEFAULT_LOW_DIR_Q_THRESHOLD = 5
DEFAULT_MAX_Q_WAIT_LOOPS = 100
DEFAULT_REQUEST_WORK_INTERVAL = 2
DEFAULT_STATS_OUTPUT_INTERVAL = 30
DEFAULT_THREAD_COUNT = 8
DEFAULT_THREADS_PER_PROC_COUNT = 4
