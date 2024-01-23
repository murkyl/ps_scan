#!/usr/bin/env python
# coding: utf-8
"""
This script can be run directly to replace the existing access zone filters in a
Kibana saved object
"""
# fmt: off
__title__         = "gen_storagepool_usage_by_path_ndjson"
__version__       = "1.0.0"
__date__          = "23 January 2024"
__license__       = "MIT"
__author__        = "Andrew Chung <andrew.chung@dell.com>"
__maintainer__    = "Andrew Chung <andrew.chung@dell.com>"
__email__         = "andrew.chung@dell.com"
# fmt: on


PATH_PANEL_UUID = "e9749654-d222-42e4-9a47-9bdfb7877256"
LAYER_UUID = "e6a65b63-2b9b-4587-ae08-87de6e3fdd80"
CLOUD_FILES_UUID = "dc7a477b-4256-4d19-aa6e-12cc94e5b8a5"
CLOUD_SIZE_UUID = "31385a4c-cce7-4653-8a7c-003071a262ef"


def main():
    import sys
    from helpers.vis_gen_helpers import create_file_path_filter
    from helpers.vis_gen_helpers import create_nodepool_count_and_size_columns
    from helpers.vis_gen_helpers import update_by_path_and_key
    from helpers.vis_gen_helpers import update_panel_columns
    from helpers.vis_gen_helpers import update_panel_path_filter

    num_args = len(sys.argv)
    if num_args < 2 or num_args > 4:
        sys.stderr.write(
            "Usage: {prog} <input_template> <path_file> [<output_file_defaults_to_stdout>]\n".format(prog=sys.argv[0])
        )
        sys.exit(1)
    infile = open(sys.argv[1], "r")
    path_file = open(sys.argv[2], "r")
    outfile = sys.stdout if num_args == 3 else open(sys.argv[3], "w")
    ndjson_array = infile.readlines()
    filter_paths = path_file.readlines()
    infile.close()
    path_file.close()

    filter_entries = create_file_path_filter(filter_paths)
    ndjson_array = update_panel_path_filter(
        ndjson_array,
        "attributes/state/datasourceStates/formBased/layers/*/columns/%s" % PATH_PANEL_UUID,
        filter_entries,
    )
    data = create_nodepool_count_and_size_columns()
    ndjson_array = update_panel_columns(
        ndjson_array,
        "attributes/state/datasourceStates/formBased/layers/%s" % LAYER_UUID,
        data,
        [PATH_PANEL_UUID, CLOUD_FILES_UUID, CLOUD_SIZE_UUID],
    )
    if not ndjson_array:
        sys.stderr.write("Unable to update the path filter\n")
        sys.exit(2)
    for line in ndjson_array:
        outfile.write(line + "\n")
    outfile.close()


if __name__ == "__main__" or __file__ == None:
    main()
