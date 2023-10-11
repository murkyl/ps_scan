#!/usr/bin/env python
# coding: utf-8
"""
Wrapper functions to OneFS change user routine
"""
# fmt: off
__title__         = "onefs_become_user"
__version__       = "1.0.0"
__date__          = "26 September 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
__all__ = [
    "become_user",
]
# fmt: on
from cffi import FFI
import os

# fmt: off
NTOKEN_UID        = 0x00000001
NTOKEN_GID        = 0x00000002
NTOKEN_SID        = 0x00000004
NTOKEN_GSID       = 0x00000008
NTOKEN_GROUPS     = 0x00000010
NTOKEN_IPRIVS     = 0x00000020
NTOKEN_LADDR      = 0x00000040
NTOKEN_RADDR      = 0x00000080
NTOKEN_PROTOCOL   = 0x00000100
NTOKEN_ZID        = 0x00000200
NTOKEN_FIELD_MASK = 0x000003FF
NTOKEN_USEUID     = 0x00010000
NTOKEN_USEGID     = 0x00020000
NTOKEN_DENYIFS    = 0x00040000
NTOKEN_BACKUP     = 0x00080000   # Used with ISI_PRIV_IFS_BACKUP
NTOKEN_RESTORE    = 0x00100000   # Used with ISI_PRIV_IFS_RESTORE
NTOKEN_FLAG_MASK  = 0x001F0000
# fmt: on


CDEF = """
struct in6_addr {
  union {
    unsigned char  _S6_u8[16];
    unsigned short _S6_u16[8];
    unsigned long  _S6_u32[4];
  } _S6_un;
};

struct isi_priv {
  unsigned long priv_id;            /* Unique priv ID */
  unsigned long priv_flags;
};

struct persona {
  unsigned char  ps_type;    /* type of persona */
  unsigned char  ps_flags;   /* flags */
  unsigned short ps_len;     /* total length */
};

struct native_token {
  unsigned int     nt_fields;     /* valid fields */
  unsigned long    nt_uid;        /* user id */
  struct persona  *nt_sid;        /* user sid */
  unsigned long    nt_gid;        /* group id */
  struct persona  *nt_gsid;       /* group sid */
  struct persona **nt_groups;     /* group personae */
  short            nt_ngroups;    /* number of groups */
  struct isi_priv *nt_iprivs;     /* isilon privileges */
  short            nt_niprivs;    /* number of isilon privileges */
  unsigned long    nt_zid;        /* zone id */
  struct in6_addr  nt_laddr;      /* local IP address */
  struct in6_addr  nt_raddr;      /* remote client IP address */
  short            nt_protocol;   /* see enum isp_protocol */
};

int modifytcred(int fd1, int fd2, int flags);
int modifytcred2(int fd1, struct native_token *token, int flags);
int gettcred(const char *user, int);
int settcred(int fd, int tcred_flags, struct native_token *token);
"""

ffi = FFI()
ffi.cdef(CDEF)
C_LIB = ffi.dlopen(None)


def become_user(user):
    cur_credentials = -1
    mod_credentials = -1
    new_credentials = -1
    tgt_credentials = -1
    try:
        cur_credentials = C_LIB.gettcred(ffi.NULL, 0)
        if cur_credentials < 0:
            raise Exception({"msg": "Unable to get current credentials"})

        tgt_credentials = C_LIB.gettcred(str(user).encode("ascii"), 0)
        if tgt_credentials < 0:
            raise Exception({"msg": "Unable to get target user credentials", "user": user})

        new_credentials = C_LIB.modifytcred(
            tgt_credentials, cur_credentials, NTOKEN_PROTOCOL | NTOKEN_LADDR | NTOKEN_RADDR
        )
        if new_credentials < 0:
            raise Exception({"msg": "Unable to modify credentials", "user": user})

        # Add capability to perform backup/restore if the user has the privileges
        empty_token = ffi.new("struct native_token *")
        empty_token.nt_fields = NTOKEN_BACKUP | NTOKEN_RESTORE
        mod_credentials = C_LIB.modifytcred2(new_credentials, empty_token, 0)
        if mod_credentials < 0:
            raise Exception({"msg": "Unable to add backup and restore privileges", "user": user})

        token = ffi.new("struct native_token *")
        err = C_LIB.settcred(mod_credentials, 0, token)
        if err < 0:
            raise Exception({"msg": "Failed to set token", "user": user})
    except Exception:
        raise
    finally:
        for fd in [cur_credentials, mod_credentials, new_credentials, tgt_credentials]:
            if fd > 0:
                os.close(fd)
