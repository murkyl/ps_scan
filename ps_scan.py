#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
OneFS file scanner
"""
# fmt: off
__title__         = "ps_scan"
__version__       = "1.0.0"
__date__          = "12 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on
import datetime
import logging
import multiprocessing as mp
import os
import queue
import sys
import threading
import time

import elasticsearch_wrapper
import scanit
import user_handlers
import helpers.misc as misc
from helpers.cli_parser import *
from helpers.constants import *

DEFAULT_LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(module)s:%(lineno)d] - (%(process)d|%(threadName)s) %(message)s"
LOG = logging.getLogger("")


def print_interim_statistics(stats, now, start):
    sys.stdout.write(
        """{ts} - Statistics:
    Current run time (s): {runtime:d}
    FPS: {fps:0.1f}
    Total file bytes processed: {f_bytes}
    Files (Processed/Queued/Skipped): {f_proc}/{f_queued}/{f_skip}
    File Q Size/Handler time: {f_q_size}/{f_h_time:0.1f}
    Dir scan time: {d_scan:0.1f}
    Dirs (Processed/Queued/Skipped): {d_proc}/{d_queued}/{d_skip}
    Dir Q Size/Handler time: {d_q_size}/{d_h_time:0.1f}
""".format(
            ts=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            runtime=int(now - start),
            d_proc=stats.get("dirs_processed", 0),
            d_h_time=stats.get("dir_handler_time", 0),
            d_q_size=stats.get("dir_q_size", 0),
            d_queued=stats.get("dirs_queued", 0),
            d_scan=stats.get("dir_scan_time", 0),
            d_skip=stats.get("dirs_skipped", 0),
            f_bytes=stats.get("file_size_total", 0),
            f_h_time=stats.get("file_handler_time", 0),
            f_proc=stats.get("files_processed", 0),
            f_q_size=stats.get("file_q_size", 0),
            f_queued=stats.get("files_queued", 0),
            f_skip=stats.get("files_skipped", 0),
            fps=stats.get("files_processed", 0) / (now - start),
        ),
    )


def print_final_statistics(stats, num_threads, wall_time, es_time):
    sys.stdout.write(
        """Final statistics
    Wall time (s): {wall_tm:.2f}
    Average Q wait time (s): {avg_q_tm:.2f}
    Total time spent in dir/file handler routines across all threads (s): {dht:.2f}/{fht:.2f}
    Processed/Queued/Skipped dirs: {p_dirs}/{q_dirs}/{s_dirs}
    Processed/Queued/Skipped files: {p_files}/{q_files}/{s_files}
    Total file size: {fsize}
    Avg files/second: {a_fps}
""".format(
            wall_tm=wall_time,
            avg_q_tm=stats["q_wait_time"] / num_threads,
            dht=stats.get("dir_handler_time", 0),
            fht=stats.get("file_handler_time", 0),
            p_dirs=stats.get("dirs_processed", 0),
            q_dirs=stats.get("dirs_queued", 0),
            s_dirs=stats.get("dirs_skipped", 0),
            p_files=stats.get("files_processed", 0),
            q_files=stats.get("files_queued", 0),
            s_files=stats.get("files_skipped", 0),
            fsize=stats.get("file_size_total", 0),
            a_fps=(stats.get("files_processed", 0) + stats.get("files_skipped", 0)) / wall_time,
        ),
    )


def setup_logger(log_obj, options):
    debug_count = options.debug
    if (options.log is None) and (not options.quiet):
        options.console_log = True
    if debug_count > 0:
        log_obj.setLevel(logging.DEBUG)
    else:
        log_obj.setLevel(logging.INFO)
    if options.console_log:
        log_handler = logging.StreamHandler()
        log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        log_obj.addHandler(log_handler)
    if options.log:
        log_handler = logging.FileHandler(options.log)
        log_handler.setFormatter(logging.Formatter(DEFAULT_LOG_FORMAT))
        log_obj.addHandler(log_handler)
    if (options.log is None) and (options.console_log is False):
        log_obj.addHandler(logging.NullHandler())


def subprocess(process_state, scan_paths, file_handler, options):
    LOG = logging.getLogger()
    setup_logger(LOG, options)
    LOG.debug("Process loop started: {pid}".format(pid=mp.current_process()))

    # Setup process local variables
    start_wall = time.time()
    cmd_poll_interval = options.cmd_poll_interval
    dir_output_count = 0
    dir_output_interval = options.dir_output_interval
    dir_request_interval = options.dir_request_interval
    es_send_thread_handles = []
    poll_interval = options.q_poll_interval
    stats_output_count = 0
    stats_output_interval = options.stats_interval
    send_data_to_es = options.es_user and options.es_pass and options.es_url and options.es_index

    # Initialize and start the scanner
    scanner = scanit.ScanIt()
    cstates = scanner.get_custom_state()
    user_handlers.init_custom_state(cstates[0], options)
    scanner.dir_chunk = options.dirq_chunk
    scanner.dir_priority_count = options.dirq_priority
    scanner.file_chunk = options.fileq_chunk
    scanner.file_q_cutoff = options.fileq_cutoff
    scanner.file_q_min_cutoff = options.fileq_min_cutoff
    scanner.handler_custom_stats = user_handlers.custom_stats_handler
    scanner.handler_file = file_handler
    scanner.handler_init_thread = user_handlers.init_thread
    scanner.num_threads = process_state.get("threads", DEFAULT_THREAD_COUNT)
    scanner.processing_type = scanit.PROCESS_TYPE_ADVANCED
    scanner.exit_on_idle = False
    scanner.add_scan_path(scan_paths)
    scanner.start()

    # Setup our own process state and inform the parent process of our status
    process_state["status"] = "running"
    process_state["dirs_requested"] = 0
    conn_pipe = process_state["child_conn"]
    conn_pipe.send([CMD_STATUS_RUN, None])
    # Send empty stats to the parent process to populate the stats data
    conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])

    # Start Elasticsearch send threads
    if send_data_to_es:
        LOG.debug("Initializing send threads for Elasticsearch")
        es_send_cmd_q = queue.Queue()
        send_q = scanner.get_custom_state()[0]["send_q"]
        for i in range(options.es_threads):
            es_thread_instance = threading.Thread(
                target=elasticsearch_wrapper.es_data_sender,
                args=(
                    send_q,
                    es_send_cmd_q,
                    options.es_url,
                    options.es_user,
                    options.es_pass,
                    options.es_index,
                ),
            )
            es_thread_instance.start()
            es_send_thread_handles.append(es_thread_instance)

    # Main processing loop
    # TODO: DEBUG: Add some code to detect when processes have died/never coming back
    # Maybe due to a memory error??
    try:
        while True:
            now = time.time()
            cur_dir_q_size = scanner.get_dir_queue_size()
            # Check for any commands from the parent process
            data_avail = conn_pipe.poll(cmd_poll_interval)
            if data_avail:
                work_item = conn_pipe.recv()
                cmd = work_item[0]
                if cmd == CMD_EXIT:
                    scanner.terminate()
                    break
                elif cmd == CMD_SEND_DIR:
                    # Parent process has sent directories to process
                    LOG.debug("CMD: SEND_DIR - Got %s dirs to process" % (len(work_item[1])))
                    process_state["status"] = "running"
                    process_state["want_data"] = 0
                    scanner.add_scan_path(work_item[1])
                    conn_pipe.send([CMD_STATUS_RUN, 0])
                    # Update our current queue size
                    cur_dir_q_size = scanner.get_dir_queue_size()
                elif cmd == CMD_REQ_DIR:
                    # Need to return some directories if possible
                    dir_list = scanner.get_dir_queue_items(percentage=work_item[1])
                    if dir_list:
                        conn_pipe.send([CMD_SEND_DIR, dir_list, now])
                elif cmd == CMD_REQ_DIR_COUNT:
                    # Return the number of directory chunks we have queued for processing
                    conn_pipe.send([CMD_SEND_DIR_COUNT, cur_dir_q_size])
                elif cmd == CMD_REQ_FILE_COUNT:
                    # Return the number of directory chunks we have queued for processing
                    conn_pipe.send([CMD_SEND_FILE_COUNT, scanner.get_file_queue_size()])
                else:
                    LOG.error("Unknown command received in process: {s}".format(s=cmd))
            # Determine if we should send a statistics update
            cur_stats_count = (now - start_wall) // stats_output_interval
            if cur_stats_count > stats_output_count:
                stats_output_count = cur_stats_count
                conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])
            # Determine if we should send a directory queue count update
            cur_dir_count = (now - start_wall) // dir_output_interval
            if cur_dir_count > dir_output_count:
                dir_output_count = cur_dir_count
                conn_pipe.send([CMD_SEND_DIR_COUNT, cur_dir_q_size])
            # Ask parent process for more data if required, limit data requests to dir_request_interval seconds
            if (cur_dir_q_size < DEFAULT_LOW_DIR_Q_THRESHOLD) and (
                now - process_state["want_data"] > dir_request_interval
            ):
                process_state["want_data"] = now
                conn_pipe.send([CMD_REQ_DIR, cur_dir_q_size, scanner.get_file_queue_size()])
            # Check if the scanner is idle
            if (
                not cur_dir_q_size
                and not scanner.get_file_queue_size()
                and not scanner.is_processing()
                and process_state["status"] != "idle"
            ):
                process_state["status"] = "idle"
                conn_pipe.send([CMD_STATUS_IDLE, cur_dir_q_size, scanner.get_file_queue_size()])
            # Small sleep to throttle polling
            time.sleep(poll_interval)
        LOG.debug("Scanner finished file scan")
        # Scanner is done processing. Wait for all the data to be sent to Elasticsearch
        if send_data_to_es:
            LOG.debug("Waiting for send queue to empty")
            send_start = now
            while not send_q.empty():
                time.sleep(poll_interval)
            es_send_q_time = now - send_start
            LOG.debug("Sending exit command to send queue")
            for thread_handle in es_send_thread_handles:
                es_send_cmd_q.put([CMD_EXIT, None])
            for thread_handle in es_send_thread_handles:
                thread_handle.join()
    except KeyboardInterrupt as kbe:
        LOG.debug("Enumerate threads: %s" % threading.enumerate())
        sys.stderr.write(
            "Termination signal received. Shutting down scanner in subprocess: {pid}.\n".format(
                pid=mp.current_process()
            )
        )
    except:
        LOG.exception("Unhandled exception in subprocess")
    finally:
        for thread_handle in es_send_thread_handles:
            es_send_cmd_q.put([CMD_EXIT, None])
        scanner.terminate(True)

    # Send statistics back to parent and end process
    total_wall_time = time.time() - start_wall
    if not send_data_to_es:
        es_send_q_time = 0
    conn_pipe.send([CMD_SEND_STATS, scanner.get_stats()])
    conn_pipe.send([CMD_EXIT, None])
    LOG.debug("Process loop ending: {pid}".format(pid=mp.current_process()))


def ps_scan(paths, options, file_handler):
    # Start local scanning processes
    start_wall = time.time()
    num_procs = options.threads // options.threads_per_proc + 1 * (options.threads % options.threads_per_proc != 0)
    base_threads = options.threads // num_procs
    poll_interval = options.q_poll_interval
    request_work_dirq_percentage = options.dirq_request_percentage
    request_work_interval = options.request_work_interval
    stats_output_count = 0
    stats_output_interval = options.stats_interval
    thread_count = options.threads
    process_states = []
    scan_path_chunks = misc.chunk_list(paths, num_procs)
    LOG.debug(
        "Starting {count} processes with {threads} threads across all processes".format(
            count=num_procs, threads=options.threads
        )
    )
    for i in range(num_procs):
        parent_conn, child_conn = mp.Pipe()
        threads_for_process = base_threads if thread_count > base_threads else thread_count
        process_state = {
            "dir_count": 0,
            "id": i + 1,
            "stats_time": None,
            "stats": {},
            "status": "starting",
            "parent_conn": parent_conn,
            "child_conn": child_conn,
            "threads": threads_for_process,
            "want_data": time.time(),
            "data_requested": 0,
        }
        proc_handle = mp.Process(
            target=subprocess,
            args=(
                process_state,
                scan_path_chunks[i],
                file_handler,
                options,
            ),
        )
        thread_count -= base_threads
        process_state["handle"] = proc_handle
        process_states.append(process_state)
        LOG.debug("Starting process: {id} with {tcount} threads".format(id=i + 1, tcount=threads_for_process))
        proc_handle.start()
    LOG.debug("All processes started")

    sys.stdout.write("Statistics interval: {si} seconds\n".format(si=options.stats_interval))
    # Main loop
    #   * Check for any commands from the sub processes
    #   * Output statistics
    #   * Check if we should exit
    dir_list = []
    continue_running = True
    while continue_running:
        try:
            now = time.time()
            # Check if there are any commands coming from the sub processes
            idle_proc = 0
            for proc in process_states:
                data_avail = True
                # DEBUG: Change to a select call to handle sockets
                while data_avail:
                    data_avail = proc["parent_conn"].poll(0.001)
                    if not data_avail:
                        continue
                    work_item = proc["parent_conn"].recv()
                    cmd = work_item[0]
                    LOG.debug("Process cmd received ({pid}): 0x{cmd:x}".format(pid=proc["id"], cmd=cmd))
                    if cmd == CMD_EXIT:
                        proc["status"] = "stopped"
                    elif cmd == CMD_SEND_STATS:
                        proc["stats"] = work_item[1]
                        proc["stats_time"] = now
                    elif cmd == CMD_REQ_DIR:
                        # A child process is requesting directories to process
                        proc["want_data"] = now
                        LOG.debug(
                            "CMD: REQ_DIR - Process (%s) wants directories. Currently gas %d dir chunks and %d file chunks queued"
                            % (proc["id"], work_item[1], work_item[2])
                        )
                    elif cmd == CMD_SEND_DIR:
                        dir_list.extend(work_item[1])
                        proc["data_requested"] = 0
                        proc["want_data"] = 0
                    elif cmd == CMD_SEND_DIR_COUNT:
                        proc["dir_count"] = work_item[1]
                    elif cmd == CMD_STATUS_IDLE:
                        proc["status"] = "idle"
                        proc["want_data"] = now
                    elif cmd == CMD_STATUS_RUN:
                        proc["status"] = "running"
                        proc["want_data"] = 0
                if proc["status"] == "idle":
                    idle_proc += 1

            # Output statistics
            #   The -1 is for a 1 second offset to allow time for stats to come from processes
            cur_stats_count = (now - start_wall - 1) // stats_output_interval
            if cur_stats_count > stats_output_count:
                temp_stats = misc.merge_process_stats(process_states) or {}
                print_interim_statistics(temp_stats, now, start_wall)
                stats_output_count = cur_stats_count
            # Check if we should exit
            continue_running = False
            for proc in process_states:
                if proc["status"] != "stopped":
                    continue_running = True
                    break
            # Check if we can terminate all the scanner processes
            if idle_proc == num_procs:
                # All sub processes are idle so we can quit the scanner
                for proc in process_states:
                    if proc["status"] != "exiting":
                        proc["parent_conn"].send([CMD_EXIT, 0])
                        proc["status"] = "exiting"
                # Skip any further processing and just wait for processes to end
                continue
            # Check if we need to request or send any directories to existing processes
            want_work_procs = []
            have_dirs_procs = []
            for proc in process_states:
                if proc["want_data"]:
                    want_work_procs.append(proc)
                # Any processes that have directories and did not request work in the last second
                # will be put on a queue to ask for directories to process
                if proc["dir_count"] and (not proc["want_data"] or proc["want_data"] > (now + 1)):
                    have_dirs_procs.append(proc)
            if want_work_procs and dir_list:
                # Send out our directories to all processes that want work evenly
                want_work_count = len(want_work_procs)
                increment = len(dir_list) // want_work_count + 1 * (len(dir_list) % want_work_count != 0)
                index = 0
                for proc in want_work_procs:
                    work_dirs = dir_list[index : index + increment]
                    if not work_dirs:
                        continue
                    proc["parent_conn"].send([CMD_SEND_DIR, work_dirs])
                    proc["want_data"] = 0
                    index += increment
                    want_work_procs.remove(proc)
                    dir_list = []
            if want_work_procs and have_dirs_procs:
                for proc in have_dirs_procs:
                    # Limit the number of times we request data from each process to request_work_interval seconds
                    if (now - proc["data_requested"]) > request_work_interval:
                        proc["parent_conn"].send([CMD_REQ_DIR, request_work_dirq_percentage])
                        proc["data_requested"] = now
            # Sleep for a short interval to slow down polling
            time.sleep(poll_interval)
        except KeyboardInterrupt as kbe:
            sys.stderr.write("Termination signal received. Shutting down scanner.\n")
            break
    total_wall_time = time.time() - start_wall
    temp_stats = misc.merge_process_stats(process_states)
    # DEBUG: Need to add ES Send Q Time back in
    print_final_statistics(temp_stats, options.threads, total_wall_time, 0)


def main():
    # Setup command line parser and parse agruments
    (parser, options, args) = parse_cli(sys.argv, __version__, __date__)

    # Setup logging
    setup_logger(LOG, options)

    # Validate command line options
    cmd_line_errors = []
    if len(args) == 0:
        cmd_line_errors.append("***** A minimum of 1 path to scan is required to be specified on the command line.")
    if options.type is None:
        cmd_line_errors.append("***** You must specify a scan type with the --type option.")
    if cmd_line_errors:
        parser.print_help()
        sys.stderr.write("\n" + "\n".join(cmd_line_errors) + "\n")
        sys.exit(1)
    if options.es_cred_file:
        try:
            with open(options.es_cred_file) as f:
                lines = f.readlines()
                options.es_user = lines[0].strip()
                options.es_pass = lines[1].strip()
                if len(lines) > 2:
                    options.es_index = lines[2].strip()
                if len(lines) > 3:
                    options.es_url = lines[3].strip()
        except:
            LOG.critical("Unable to open or read the credentials file: {file}".format(file=options.es_cred_file))
            sys.exit(3)
    if options.type == "onefs":
        if not misc.is_onefs_os():
            sys.stderr.write(
                "Script is not running on a OneFS operation system. Invalid --type option, use 'basic' instead.\n"
            )
            sys.exit(2)
        file_handler = user_handlers.file_handler_pscale
    else:
        file_handler = user_handlers.file_handler_basic
    LOG.debug("Parsed options: {opt}".format(opt=options))

    es_client = None
    if options.es_index and options.es_user and options.es_pass and options.es_url:
        es_client = elasticsearch_wrapper.es_create_connection(
            options.es_url, options.es_user, options.es_pass, options.es_index
        )
    if es_client and (options.es_init_index or options.es_reset_index):
        if options.es_reset_index:
            elasticsearch_wrapper.es_delete_index(es_client)
        LOG.debug("Initializing indices for Elasticsearch: {index}".format(index=options.es_index))
        elasticsearch_wrapper.es_init_index(es_client, options.es_index)
    if es_client:
        elasticsearch_wrapper.es_start_processing(es_client, options)
    # Start scanner
    ps_scan(args, options, file_handler)
    if es_client:
        elasticsearch_wrapper.es_stop_processing(es_client, options)


if __name__ == "__main__" or __file__ == None:
    # Support scripts built into executable on Windows
    try:
        mp.freeze_support()
        if hasattr(mp, "set_start_method"):
            # Force all OS to behave the same when spawning new process
            mp.set_start_method("spawn")
    except:
        # Ignore these errors as they are either already set or do not apply to this system
        pass
    main()
