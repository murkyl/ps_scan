#!/usr/bin/env python
# -*- coding: utf8 -*-
# fmt: off
__title__         = "create_ps_test_data"
__version__       = "1.0.0"
__date__          = "21 August 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on

import datetime
import optparse
import os
import platform
import queue
import random
import resource
import subprocess
import sys
import threading
import time

USAGE = "usage: %prog [OPTION...] PATH... [PATH..]"
EPILOG = """
TODO: Add help here
"""
DATE_1D = 3600 * 24
DATE_1Y = 365 * DATE_1D
DATA_BUFFER = None  # Initialize this buffer after seeding
DATA_BUFFER_LENGTH = 1024 * 1024 * 1024
SIZE_1Ki = 1024
SIZE_1Mi = SIZE_1Ki * SIZE_1Ki
SIZE_1Gi = SIZE_1Mi * SIZE_1Ki
SIZE_1Ti = SIZE_1Gi * SIZE_1Ki
DEFAULT_DATE_DIST = [
    datetime.timedelta(days=1),  # 1 day
    datetime.timedelta(days=30),  # 1 month
    datetime.timedelta(days=92),  # 3 month
    datetime.timedelta(days=274),  # 9 months
    datetime.timedelta(days=365),  # 1 year
    datetime.timedelta(days=1461),  # 4 years
]
DEFAULT_FILENAME_TEMPLATE = "file_{fn}{ext}"
DEFAULT_MAX_WRITE_SIZE = 1024 * 1024
DEFAULT_SIZE_DIST = [
    (1 * SIZE_1Ki, 50),
    (10 * SIZE_1Ki, 30),
    (100 * SIZE_1Ki, 18),
    (1 * SIZE_1Mi, 1),
    (1 * SIZE_1Gi, 1),
]
DEFAULT_THREAD_COUNT = 10
DEFAULT_USER_DIST = [
    [2017, 1800], # ftpuser3
    [2018, 1800], # ftpuser4
    [12002, 12000], # isilon.com\sally
    [20002, 11000], # isilon.com\devops1
]
EXTENSIONS = [
    "cpp",
    "db",
    "docx",
    "exe",
    "go",
    "iso",
    "jpg",
    "msg",
    "mp4",
    "pdf",
    "png",
    "ps1",
    "sh",
    "tgz",
    "txt",
    "xlsx",
    "wav",
    "zip",
]
EXCLUDE_USERS = [
    "_lldpd",
    "compadmin",
    "git_daemon",
    "nobody",
    "Guest",
    "www",
]


def create_date_files(paths, size_dist, date_dist, options={}):
    distribution = []
    filename_template = options.get("filename_template", DEFAULT_FILENAME_TEMPLATE)
    filename_extensions = options.get("filename_extensions", EXTENSIONS)
    file_count = 0
    file_q = queue.Queue()
    num_files = options.get("num_files", 100)
    remaining_files = num_files
    simulate = options.get("simulate", False)
    thread_handles = []
    threads = options.get("threads", DEFAULT_THREAD_COUNT)
    total_percent = 0

    for size in size_dist:
        target_file_count = int(round((size[1] * num_files) / 100.0))
        if target_file_count > remaining_files:
            target_file_count = remaining_files
        if target_file_count < 1:
            target_file_count = 1
        distribution.append(
            {
                "size": size[0],
                "num_files": target_file_count,
            }
        )
        total_percent += size[1]
        remaining_files -= target_file_count
    if total_percent != 100:
        raise Exception("Invalid percentage in size distribution. Values must add to 100.")
    if remaining_files:
        max_pct = 0
        max_idx = 0
        for i in range(len(size_dist)):
            if size_dist[i][0] > max_pct:
                max_pct = size_dist[i][0]
                max_idx = i
        distribution[max_idx]["num_files"] += remaining_files

    # Start up threads
    for i in range(threads):
        handle = threading.Thread(target=write_data, kwargs={"file_q": file_q})
        thread_handles.append(handle)
        handle.daemon = True
        handle.start()

    for p in paths:
        for i in range(num_files):
            bucket = distribution[random.randrange(0, len(distribution))]
            bucket["num_files"] -= 1
            if bucket["num_files"] < 1:
                distribution.remove(bucket)
            file_ext = ""
            if filename_extensions:
                file_ext = "." + filename_extensions[random.randrange(0, len(filename_extensions))]
            filename = filename_template.format(fn=file_count, ext=file_ext)
            file_count += 1
            if simulate:
                print("Filename: {fn}, size: {size}".format(fn=filename, size=bucket["size"]))
            else:
                file_q.put({"filename": os.path.join(p, filename), "size": bucket["size"]})
    for i in range(threads):
        file_q.put(None)
    stats_interval = 0
    start_time = time.time()
    while file_q.qsize() > 0:
        stats_count = (time.time() - start_time) // 2
        if stats_count > stats_interval:
            print("File create queue size: {size}".format(size=file_q.qsize()))
            stats_interval = stats_count
        time.sleep(1)


def create_inline_data_files(num_files, size_dist, user_list):
    pass


def create_dedupe_files(dedupe_pct, num_files, size_dist, user_list):
    pass


def create_sparse_files():
    pass


def get_cluster_users():
    pass


def is_onefs_os():
    return "OneFS" in platform.system()


def set_resource_limits(min_memory=1024 * 1024 * 1024 * 4, force=False):
    old_limit = None
    new_limit = None
    if not is_onefs_os() and not force:
        return (None, None)
    try:
        old_limit = resource.getrlimit(resource.RLIMIT_VMEM)
    except Exception as e:
        pass
    try:
        physmem = int(sysctl("hw.physmem"))
    except Exception as e:
        physmem = 0
    try:
        if physmem >= min_memory or force:
            if old_limit is None or min_memory > old_limit[1]:
                resource.setrlimit(resource.RLIMIT_VMEM, (min_memory, min_memory))
                new_limit = min_memory
            else:
                new_limit = old_limit
    except Exception as e:
        return (None, None)
    return (old_limit, new_limit)


def write_data(file_q):
    now = datetime.date.today()
    date_dist = DEFAULT_DATE_DIST
    date_dist_len = len(date_dist)
    user_dist = DEFAULT_USER_DIST
    user_dist_len = len(user_dist)

    while True:
        work_item = file_q.get()
        if work_item is None:
            break
        bytes_left = work_item["size"]
        with open(work_item["filename"], "wb") as fh:
            # open file
            # loop through random parts of the data buffer
            # write bytes until bytes_left = 0
            while bytes_left > 0:
                offset = random.randrange(0, DATA_BUFFER_LENGTH - SIZE_1Ki)
                max_length = DATA_BUFFER_LENGTH - offset - 1
                write_bytes = bytes_left if bytes_left < max_length else max_length
                if write_bytes > DEFAULT_MAX_WRITE_SIZE:
                    write_bytes = DEFAULT_MAX_WRITE_SIZE
                try:
                    fh.write(DATA_BUFFER[offset : offset + write_bytes])
                except Exception as e:
                    print(
                        "Could not write to file: {fn}. Size: {size}, Offset: {offset}, Bytes: {bytes}, End byte: {end}\nErr: {err}".format(
                            fn=work_item["filename"],
                            size=work_item["size"],
                            offset=offset,
                            bytes=write_bytes,
                            end=(offset + write_bytes),
                            err=str(e),
                        )
                    )
                bytes_left -= write_bytes
        # Change the ownership of the file
        user_group = user_dist[random.randrange(0, user_dist_len)]
        os.chown(work_item["filename"], user_group[0], user_group[1])
        # Change the atime and mtime of the file
        timestamp = time.mktime((now - date_dist[random.randrange(0, date_dist_len)]).timetuple())
        os.utime(work_item["filename"], (timestamp, timestamp))


def main():
    global DATA_BUFFER
    # Setup command line parser and parse agruments
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)
    random.seed(options.get("seed", 0))
    DATA_BUFFER = os.urandom(DATA_BUFFER_LENGTH)
    if options["type"] == "rand_date":
        create_date_files(
            args,
            DEFAULT_SIZE_DIST,
            DEFAULT_DATE_DIST,
            options,
        )


def parse_cli(argv, prog_ver, prog_date):
    # Create our command line parser. We use the older optparse library for compatibility on OneFS
    optparse.OptionParser.format_epilog = lambda self, formatter: self.epilog
    parser = optparse.OptionParser(
        usage=USAGE,
        version="%prog v" + prog_ver + " (" + prog_date + ")",
        epilog=EPILOG,
    )
    parser.add_option(
        "--type",
        type="choice",
        choices=("rand_date", "sparse", "dedupe50"),
        default="rand_date",
        help="""File creation type to use. Default: rand_date         
rand_date: Create files with a random distribution    
    of dates based on a date array.                   
""",
    )
    parser.add_option(
        "--num_files",
        type="int",
        default=5000,
        help="""Number of files to create.                            
""",
    )
    parser.add_option(
        "--seed",
        type="int",
        default=int(time.time()),
        help="""Random number seed. Default: current time             
""",
    )
    parser.add_option(
        "--simulate",
        action="store_true",
        default=False,
        help="""Output files that would be created to screen.         
""",
    )
    parser.add_option(
        "--threads",
        type="int",
        default=2,
        help="""Number of write threads. Default: 2                   
""",
    )
    parser.add_option(
        "--debug",
        default=0,
        action="count",
        help="Add multiple debug flags to increase debug",
    )
    (raw_options, args) = parser.parse_args(argv[1:])
    return (parser, raw_options.__dict__, args)


if __name__ == "__main__" or __file__ == None:
    set_resource_limits(force=True)
    main()
