#!/usr/bin/env python
# coding: utf-8
"""
This script can be run directly to replace the existing access zone filters in a
Kibana saved object
"""
# fmt: off
__title__         = "gen_storage_usage_by_path_ndjson"
__version__       = "1.0.0"
__date__          = "17 January 2024"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on


PANEL_UUID = "e9749654-d222-42e4-9a47-9bdfb7877256"


def main():
    import sys
    from helpers.vis_gen_helpers import update_panel_path_filter
    from helpers.vis_gen_helpers import create_file_path_filter

    num_args = len(sys.argv)
    if num_args < 2 or num_args > 4:
        sys.stderr.write(
            "Usage: {prog} <input_template> <path_file> [<output_file_defaults_to_stdout>]\n".format(prog=sys.argv[0])
        )
        sys.exit(1)
    infile = open(sys.argv[1], "r")
    path_file = open(sys.argv[2], "r")
    outfile = sys.stdout if num_args == 3 else open(sys.argv[3], "w")
    template_array = infile.readlines()
    filter_paths = path_file.readlines()
    infile.close()
    panel_uuid = PANEL_UUID

    filter_entries = create_file_path_filter(filter_paths)
    ndjson_array = update_panel_path_filter(
        template_array,
        "attributes/state/datasourceStates/formBased/layers/*/columns/%s" % panel_uuid,
        filter_entries,
    )
    if not ndjson_array:
        sys.stderr.write("Unable to update the path filter\n")
        sys.exit(2)
    for line in ndjson_array:
        outfile.write(line + "\n")
    outfile.close()


if __name__ == "__main__" or __file__ == None:
    main()
