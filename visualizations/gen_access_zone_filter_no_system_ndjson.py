#!/usr/bin/env python
# coding: utf-8
"""
This script can be run directly to replace the existing access zone filters in a
Kibana saved object
"""
# fmt: off
__title__         = "gen_access_zone_filter_ndjson"
__version__       = "1.0.0"
__date__          = "08 June 2023"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on


def main():
    import sys
    from helpers.vis_gen_helpers import generate_access_zone_filter_ndjson

    num_args = len(sys.argv)
    if num_args < 2 or num_args > 3:
        sys.stderr.write("Usage: {prog} <input_template> [<output_file_defaults_to_stdout>]\n".format(prog=sys.argv[0]))
        sys.exit(1)
    infile = open(sys.argv[1], "r")
    outfile = sys.stdout if num_args == 2 else open(sys.argv[2], "w")
    template_array = infile.readlines()
    infile.close()
    ndjson_array = generate_access_zone_filter_ndjson(template_array, skip_system=True)
    for line in ndjson_array:
        outfile.write(line + "\n")
    outfile.close()


if __name__ == "__main__" or __file__ == None:
    main()
