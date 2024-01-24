## Kibana Dev Tools
```
GET _search
{
  ...
  QUERY_STRING
  ...
}
```

## cURL Commands 
```shell
curl --user elastic -XGET http://elasticsearch-6-122.isilon.com:9200/_search -H 'Content-Type: application/json' -d @- << EOF
{
  ...
  QUERY_STRING
  ...
}
EOF
```
