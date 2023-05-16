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
STAT_BLOCK_SIZE = 512
STATS_FPS_BUCKETS = [2, 5, 10]
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
DEFAULT_ES_SHARDS = 4
DEFAULT_ES_THREADS = 4
DEFAULT_LOW_DIR_Q_THRESHOLD = 5
DEFAULT_MAX_Q_WAIT_LOOPS = 100
DEFAULT_REQUEST_WORK_INTERVAL = 2
DEFAULT_SEND_Q_WAIT_TIME = 300
DEFAULT_STATS_OUTPUT_INTERVAL = 30
DEFAULT_THREAD_COUNT = 8
DEFAULT_THREADS_PER_PROC_COUNT = 4
DEFAULT_ULIMIT_MEMORY = 4 * (1024 * 1024 * 1024)
DEFAULT_ULIMIT_MEMORY_MIN = 32 * (1024 * 1024 * 1024)
