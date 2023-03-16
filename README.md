# ps_scan

This script can be used on a PowerScale cluster to perform a detailed data gathering of files on the cluster and send the data to an Elasticsearch 

# Quickstart
Create a credentials file and populate it with the following information, one item per line followed by a CR/LF pair:

    <elastic_search_username>
    <elastic_search_password>
    <elastic_search_index_name>
    <elastic_search_url_with_port>

An example file would be:

    elastic_search_user
    itsasecret
    cluster_name_index
    https://my.elasticsearch.com:9200

For the initial run, the index need to be created and that requires an extra command line option --es-init-index
`python ps_scan.py --type=onefs --es-init-index --es-cred-file=credentials.txt /ifs`

An example for a normal scan run the follows:
`python ps_scan.py --type=onefs --es-cred-file=credentials.txt /ifs`

# Issues, bug reports, and suggestions

For any issues with the script, please re-run the script with debug enabled (--debug command line option) and send the entire output along with the command line used to:

Andrew Chung <andrew.chung@dell.com>

