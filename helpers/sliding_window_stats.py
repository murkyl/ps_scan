#!/usr/bin/env python
# -*- coding: utf8 -*-
"""
Short history stats tracker
"""
# fmt: off
__title__         = "sliding_window_stats"
__version__       = "1.0.0"
__date__          = "21 April 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on


class SlidingWindowStats():
    def __init__(self, window_sizes):
        self.first_sample = True
        self.window_sizes = window_sizes
        self.buckets = [[]] * len(window_sizes)
        self._init_buckets()

    def _init_buckets(self):
        for i in range(len(self.buckets)):
            self.buckets[i] = [0] * self.window_sizes[i]

    def add_sample(self, sample):
        for bucket in self.buckets:
            bucket.append(sample)
            bucket.pop(0)

    def get_all_windows(self):
        window_stats = []
        for i in range(len(self.buckets)):
            window_stats.append(self.get_window(i))
        return window_stats

    def get_window(self, bucket_idx):
        total = 0
        window_size = self.window_sizes[bucket_idx]
        bucket = self.buckets[bucket_idx]
        for val in bucket:
            total += val
        return total/window_size

    def get_window_sizes(self):
        return list(self.window_sizes)
