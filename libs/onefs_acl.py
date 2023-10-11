#!/usr/bin/env python
# coding: utf-8
"""
Wrapper functions to OneFS ACL routines
"""
# fmt: off
__title__         = "onefs_acl"
__version__       = "1.0.0"
__date__          = "10 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "flags_to_text_list",
    "get_acl_dict",
    "get_dir_acl",
    "get_file_acl",
    "get_sd_text",
    "perms_to_text_list",
    "trustees_txt_to_aces",
]
# fmt: on
import os
import re
import functools
from cffi import FFI

SD_ACL_REGEX = re.compile(
    r"header:(revision:\d+)::(control:\d+)"
    r"::(owner:(?P<utype>UID|SID):(?P<user>\d+|S(-\d+)+))"
    r"::(group:(?P<gtype>GID|SID):(?P<group>\d+|S(-\d+)+))"
    r"::->dacl<-:(rev:(?P<daclrev>\d+)::)?(?P<dacltrustees>::trustee.*)?"
    r"::->sacl<-:(rev:(?P<saclrev>\d+)::)?(?P<sacltrustees>::trustee.*)?"
)

# fmt: off
""" Mask bits for the trustee fields"""
# ===== ACE 'perms' mask bits
ACE_PERMS_GENERIC_MASK           = 0xF0000000
ACE_PERMS_GENERIC_ALL            = 0x10000000
ACE_PERMS_GENERIC_EXECUTE        = 0x20000000
ACE_PERMS_GENERIC_WRITE          = 0x40000000
ACE_PERMS_GENERIC_READ           = 0x80000000
# Special and SACL bits
ACE_PERMS_SPECIAL_MASK           = 0x0F000000
ACE_PERMS_SACL_ACCESS            = 0x01000000
ACE_PERMS_MAXIMUM_ALLOWED        = 0x02000000
# Standard bits for directories and files
ACE_PERMS_STD_MASK               = 0x00FF0000
ACE_PERMS_STD_DELETE             = 0x00010000
ACE_PERMS_STD_READ_DAC           = 0x00020000
ACE_PERMS_STD_WRITE_DAC          = 0x00040000
ACE_PERMS_STD_WRITE_OWNER        = 0x00080000
ACE_PERMS_STD_SYNCHRONIZE        = 0x00100000 # ignored by OneFS
ACE_PERMS_STD_REQUIRED           = 0x000F0000
ACE_PERMS_STD_ALL                = 0x001F0000
ACE_PERMS_STD_EXECUTE            = ACE_PERMS_STD_READ_DAC
ACE_PERMS_STD_READ               = ACE_PERMS_STD_READ_DAC
ACE_PERMS_STD_WRITE              = ACE_PERMS_STD_READ_DAC
# Filesystem specific
ACE_PERMS_SPECIFIC_MASK          = 0x0000FFFF
ACE_PERMS_FILE_READ_DATA         = 0x00000001
ACE_PERMS_FILE_WRITE_DATA        = 0x00000002
ACE_PERMS_FILE_APPEND_DATA       = 0x00000004
ACE_PERMS_FILE_READ_EA           = 0x00000008
ACE_PERMS_FILE_WRITE_EA          = 0x00000010
ACE_PERMS_FILE_EXECUTE           = 0x00000020
ACE_PERMS_FILE_DELETE_CHILD      = 0x00000040
ACE_PERMS_FILE_READ_ATTRIBUTES   = 0x00000080
ACE_PERMS_FILE_WRITE_ATTRIBUTES  = 0x00000100
ACE_PERMS_FILE_ALL               = 0x000001FF
ACE_PERMS_DIR_LIST               = 0x00000001
ACE_PERMS_DIR_ADD_FILE           = 0x00000002
ACE_PERMS_DIR_ADD_SUBDIR         = 0x00000004
ACE_PERMS_DIR_READ_EA            = 0x00000008
ACE_PERMS_DIR_WRITE_EA           = 0x00000010
ACE_PERMS_DIR_TRAVERSE           = 0x00000020
ACE_PERMS_DIR_DELETE_CHILD       = 0x00000040
ACE_PERMS_DIR_READ_ATTRIBUTES    = 0x00000080
ACE_PERMS_DIR_WRITE_ATTRIBUTES   = 0x00000100
ACE_PERMS_DIR_ALL                = 0x000001FF
# Typical filesystem combinations
ACE_PERMS_DIR_GEN_ALL = \
    ACE_PERMS_STD_ALL | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    ACE_PERMS_DIR_ALL | \
    0

ACE_PERMS_DIR_GEN_EXECUTE = \
    ACE_PERMS_STD_EXECUTE | \
    ACE_PERMS_DIR_TRAVERSE | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_DIR_GEN_READ = \
    ACE_PERMS_STD_READ | \
    ACE_PERMS_DIR_LIST | \
    ACE_PERMS_DIR_READ_ATTRIBUTES | \
    ACE_PERMS_DIR_READ_EA | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_DIR_GEN_WRITE = \
    ACE_PERMS_STD_WRITE | \
    ACE_PERMS_DIR_ADD_FILE | \
    ACE_PERMS_DIR_ADD_SUBDIR | \
    ACE_PERMS_DIR_WRITE_ATTRIBUTES | \
    ACE_PERMS_DIR_WRITE_EA | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_DIR_MODIFYING = \
    ACE_PERMS_STD_DELETE | \
    ACE_PERMS_STD_WRITE_DAC | \
    ACE_PERMS_STD_WRITE_OWNER | \
    ACE_PERMS_DIR_ADD_FILE | \
    ACE_PERMS_DIR_ADD_SUBDIR | \
    ACE_PERMS_DIR_WRITE_EA | \
    ACE_PERMS_DIR_WRITE_ATTRIBUTES | \
    ACE_PERMS_DIR_DELETE_CHILD | \
    0

ACE_PERMS_FILE_GEN_ALL = \
    ACE_PERMS_STD_ALL | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    ACE_PERMS_FILE_ALL | \
    0

ACE_PERMS_FILE_GEN_EXECUTE = \
    ACE_PERMS_STD_EXECUTE | \
    ACE_PERMS_FILE_EXECUTE | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_FILE_GEN_READ = \
    ACE_PERMS_STD_READ | \
    ACE_PERMS_FILE_READ_DATA | \
    ACE_PERMS_FILE_READ_ATTRIBUTES | \
    ACE_PERMS_FILE_READ_EA | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_FILE_GEN_WRITE = \
    ACE_PERMS_STD_WRITE | \
    ACE_PERMS_FILE_WRITE_DATA | \
    ACE_PERMS_FILE_WRITE_ATTRIBUTES | \
    ACE_PERMS_FILE_WRITE_EA | \
    ACE_PERMS_FILE_APPEND_DATA | \
    ACE_PERMS_STD_SYNCHRONIZE | \
    0

ACE_PERMS_FILE_MODIFYING = \
    ACE_PERMS_STD_DELETE | \
    ACE_PERMS_STD_WRITE_DAC | \
    ACE_PERMS_STD_WRITE_OWNER | \
    ACE_PERMS_FILE_WRITE_DATA | \
    ACE_PERMS_FILE_APPEND_DATA | \
    ACE_PERMS_FILE_WRITE_EA | \
    ACE_PERMS_FILE_WRITE_ATTRIBUTES | \
    0

ACE_PERMS_FULL_CONTROL = \
    ACE_PERMS_STD_ALL | \
    ACE_PERMS_FILE_ALL | \
    ACE_PERMS_DIR_ALL | \
    0

# Privilege granted rights
ACE_PERMS_DIR_BACKUP_PRIV = \
    ACE_PERMS_DIR_GEN_READ | \
    ACE_PERMS_DIR_GEN_EXECUTE | \
    ACE_PERMS_SACL_ACCESS | \
    0

ACE_PERMS_DIR_RESTORE_PRIV = \
    ACE_PERMS_DIR_GEN_WRITE | \
    ACE_PERMS_DIR_DELETE_CHILD | \
    ACE_PERMS_STD_DELETE | \
    ACE_PERMS_STD_WRITE_DAC | \
    ACE_PERMS_STD_WRITE_OWNER | \
    ACE_PERMS_SACL_ACCESS | \
    0

ACE_PERMS_FILE_BACKUP_PRIV = \
    ACE_PERMS_FILE_GEN_READ | \
    ACE_PERMS_FILE_GEN_EXECUTE | \
    ACE_PERMS_SACL_ACCESS | \
    0

ACE_PERMS_FILE_RESTORE_PRIV = \
    ACE_PERMS_FILE_GEN_WRITE | \
    ACE_PERMS_STD_DELETE | \
    ACE_PERMS_STD_WRITE_DAC | \
    ACE_PERMS_STD_WRITE_OWNER | \
    ACE_PERMS_SACL_ACCESS | \
    0

# ===== ACE 'flags' mask bits
ACE_FLAG_OBJECT_INHERIT          = 0x01
ACE_FLAG_CONTAINER_INHERIT       = 0x02
ACE_FLAG_NO_PROPAGATE_INHERIT    = 0x04
ACE_FLAG_INHERIT_ONLY            = 0x08
ACE_FLAG_INHERITED_ACE           = 0x10
ACE_FLAG_VALID_INHERIT           = 0x1f
ACE_FLAG_SUCCESSFUL_ACCESS       = 0x40 # ignored by OneFS
ACE_FLAG_FAILED_ACCESS           = 0x80 # ignored by OneFS
# ===== ACE 'ifs_flags' mask bits
ACE_IFS_FLAG_KNOWN_MASK          = 0x00
ACE_IFS_FLAG_ACE_MAX             = 0xFF
# ===== ACE 'type'
ACE_TYPE_ACCESS_ALLOWED          = 0
ACE_TYPE_ACCESS_DENIED           = 1
ACE_TYPE_SYSTEM_AUDIT            = 2    # ignored by OneFS
ACE_TYPE_SYSTEM_ALARM            = 3    # ignored by OneFS
ACE_TYPE_ALLOWED_COMPOUND        = 4    # unused by OneFS
ACE_TYPE_ACCESS_ALLOWED_OBJECT   = 5    # unused by OneFS
ACE_TYPE_ACCESS_DENIED_OBJECT    = 6    # unused by OneFS
ACE_TYPE_SYSTEM_AUDIT_OBJECT     = 7    # ignored by OneFS
ACE_TYPE_SYSTEM_ALARM_OBJECT     = 8    # ignored by OneFS
# fmt: on
ACE_GROUP_BITS_STR_MAP = [
    [ACE_PERMS_FILE_GEN_ALL, "gen_all"],
    [ACE_PERMS_FILE_GEN_WRITE, "gen_write"],
    [ACE_PERMS_FILE_GEN_READ, "gen_read"],
    [ACE_PERMS_FILE_GEN_EXECUTE, "execute"],
    [ACE_PERMS_FILE_MODIFYING, "modify"],
    [ACE_PERMS_DIR_MODIFYING, "modify"],
]
ACE_SINGLE_BITS_STR_MAP = [
    [ACE_PERMS_STD_DELETE, "std_delete"],
    [ACE_PERMS_STD_READ_DAC, "std_read_dac"],
    [ACE_PERMS_STD_WRITE_DAC, "std_write_dac"],
    [ACE_PERMS_STD_WRITE_OWNER, "std_write_owner"],
    [ACE_PERMS_STD_SYNCHRONIZE, "std_synchronize"],
    [ACE_PERMS_STD_REQUIRED, "std_required"],
    [ACE_PERMS_FILE_WRITE_ATTRIBUTES, "write_attr"],
    [ACE_PERMS_FILE_READ_ATTRIBUTES, "read_attr"],
    [ACE_PERMS_FILE_DELETE_CHILD, "delete_child"],
    [ACE_PERMS_FILE_EXECUTE, "execute"],
    [ACE_PERMS_FILE_WRITE_EA, "write_ext_attr"],
    [ACE_PERMS_FILE_READ_EA, "read_ext_attr"],
    [ACE_PERMS_FILE_APPEND_DATA, "append"],
    [ACE_PERMS_FILE_WRITE_DATA, "file_write"],
    [ACE_PERMS_FILE_READ_DATA, "file_read"],
    [ACE_PERMS_SACL_ACCESS, "sacl_access"],
]
ACE_FLAG_STR_MAP = [
    [ACE_FLAG_OBJECT_INHERIT, "object_inherit"],
    [ACE_FLAG_CONTAINER_INHERIT, "container_inherit"],
    [ACE_FLAG_NO_PROPAGATE_INHERIT, "no_prop_inherit"],
    [ACE_FLAG_INHERIT_ONLY, "inherit_only"],
    [ACE_FLAG_INHERITED_ACE, "inherited_ace"],
    [ACE_FLAG_SUCCESSFUL_ACCESS, "successful_access"],
    [ACE_FLAG_FAILED_ACCESS, "failed_access"],
]
ACE_TYPE_FLAG_STR_MAP = {
    "0": "allow",
    "1": "deny",
    "2": "audit",
    "3": "alarm",
    "4": "allowed_compound",
    "5": "access_allowed_object",
    "6": "access_denied_object",
    "7": "system_audit_object",
    "8": "system_alarm_object",
    "254": "posix_mask",
}

ACE_GROUP_BITS_STR_DICT = {bitmask[0]: bitmask[1] for bitmask in ACE_GROUP_BITS_STR_MAP}
ACE_SINGLE_BITS_STR_DICT = {bitmask[0]: bitmask[1] for bitmask in ACE_SINGLE_BITS_STR_MAP}
ACE_FLAG_STR_DICT = {bitmask[0]: bitmask[1] for bitmask in ACE_FLAG_STR_MAP}

try:
    ffi = FFI()
    ffi.cdef(
        """
        char * get_dir_acl(int fd);
        char * get_file_acl(int fd);
        char * get_sd_text(int fd);
        void free(void *);
    """
    )
    C_LIB = ffi.dlopen(None)
    ISI_ACL_LIB = ffi.dlopen("libisi_acl.so")
except:
    pass


def simple_cache(maxsize):
    def decorator(func):
        cache = {}
        @functools.wraps(func)
        def wrapper(*args):
            if args not in cache:
                if len(cache) >= maxsize:
                    cache.popitem()
                cache[args] = func(*args)
            return cache[args]
        return wrapper
    return decorator


@simple_cache(maxsize=1024)
def flags_to_text_list(flags):
    cur_bits = flags
    text_labels = []
    for bitmask, label in ACE_FLAG_STR_DICT.items():
        if cur_bits & bitmask == bitmask:
            text_labels.append(label)
    return sorted(set(text_labels))


def get_acl_dict(fd, detailed=True):
    sd_str = get_sd_text(fd)
    if sd_str is None:
        return {}
    match = SD_ACL_REGEX.match(sd_str)
    if not match:
        return {}
    acl_dict = {
        "aces": trustees_txt_to_aces(match.group("dacltrustees")),
        "group": match.group("group"),
        "group_type": match.group("gtype").lower(),
        "user": match.group("user"),
        "user_type": match.group("utype").lower(),
    }
    sacl_aces = match.group("sacltrustees")
    if sacl_aces:
        acl_dict["sacl_aces"] = trustees_txt_to_aces(sacl_aces)
    for ace in acl_dict.get("aces", []):
        ace["flags_list"] = flags_to_text_list(ace["flags"])
        ace["perms_list"] = perms_to_text_list(ace["perms"], detailed)
    return acl_dict


def get_dir_acl(fd):
    retval = None
    acl = ISI_ACL_LIB.get_dir_acl(fd)
    if acl != ffi.NULL:
        retval = str(ffi.string(acl))
    C_LIB.free(acl)
    return retval


def get_file_acl(fd):
    retval = None
    acl = ISI_ACL_LIB.get_file_acl(fd)
    if acl != ffi.NULL:
        retval = str(ffi.string(acl))
    C_LIB.free(acl)
    return retval


def get_sd_text(fd):
    retval = None
    sd = ISI_ACL_LIB.get_sd_text(fd)
    if sd != ffi.NULL:
        retval = ffi.string(sd).decode("utf-8")
    C_LIB.free(sd)
    return retval


@simple_cache(maxsize=1024)
def perms_to_text_list(perms, detailed=True):
    cur_bits = perms
    group_mask = 0
    text_labels = []
    for bitmask, label in ACE_GROUP_BITS_STR_DICT.items():
        if cur_bits & bitmask == bitmask:
            group_mask |= bitmask
            text_labels.append(label)
    if group_mask and not detailed:
        cur_bits = cur_bits & ~group_mask
    for bitmask, label in ACE_SINGLE_BITS_STR_DICT.items():
        if cur_bits & bitmask == bitmask:
            text_labels.append(label)
    return sorted(set(text_labels))


def trustees_txt_to_aces(trustee_str):
    aces = []
    if not trustee_str or not trustee_str.startswith("::trustee:"):
        return aces
    trustee_list = trustee_str[10:].split("::trustee:")
    for trustee in trustee_list:
        parts = trustee.split(":")
        aces.append(
            {
                "entity": parts[1],
                "entity_type": parts[0].lower(),
                "flags": int(parts[4]),
                "flags_list": [],
                "ifs_flags": int(parts[5]),
                "ifs_flags_list": [],
                "perm_type": int(parts[2]),
                "perm_type_str": ACE_TYPE_FLAG_STR_MAP.get(parts[2], parts[2]),
                "perms": int(parts[3]),
                "perms_list": [],
            }
        )
    return aces
