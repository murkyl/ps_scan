# Storage pool usage
### QUERY STRING
```json
{
  "size": 0,
  "aggs": {
    "a300_60tb_3.2tb-ssd_96gb": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "pool_target_data_name": "a300_60tb_3.2tb-ssd_96gb"
              }
            },
            {
              "match": {
                "file_is_smartlinked": false
              }
            }
          ]
        }
      },
      "aggs": {
        "Number of files": {
          "value_count": {
            "field": "pool_target_data_name"
          }
        },
        "Physical data size": {
          "sum": {
            "field": "size_physical"
          }
        },
        "Application logical size": {
          "sum": {
            "field": "size"
          }
        },
        "Average of size (app logical)": {
          "avg": {
            "field": "size"
          }
        }
      }
    },
    "f200_7.5tb-ssd_96gb": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "pool_target_data_name": "f200_7.5tb-ssd_96gb"
              }
            },
            {
              "match": {
                "file_is_smartlinked": false
              }
            }
          ]
        }
      },
      "aggs": {
        "Number of files": {
          "value_count": {
            "field": "pool_target_data_name"
          }
        },
        "Physical data size": {
          "sum": {
            "field": "size_physical"
          }
        },
        "Application logical size": {
          "sum": {
            "field": "size"
          }
        },
        "Average of size (app logical)": {
          "avg": {
            "field": "size"
          }
        }
      }
    },
    "h40_120tb_3.2tb-ssd_128gb": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "pool_target_data_name": "h40_120tb_3.2tb-ssd_128gb"
              }
            },
            {
              "match": {
                "file_is_smartlinked": false
              }
            }
          ]
        }
      },
      "aggs": {
        "Number of files": {
          "value_count": {
            "field": "pool_target_data_name"
          }
        },
        "Physical data size": {
          "sum": {
            "field": "size_physical"
          }
        },
        "Application logical size": {
          "sum": {
            "field": "size"
          }
        },
        "Average of size (app logical)": {
          "avg": {
            "field": "size"
          }
        }
      }
    }
  }
}
```

### OUTPUT
```json
{
  "took": 3,
  "timed_out": false,
  "_shards": {
    "total": 18,
    "successful": 18,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 10000,
      "relation": "gte"
    },
    "max_score": null,
    "hits": []
  },
  "aggregations": {
    "h40_120tb_3.2tb-ssd_128gb": {
      "doc_count": 99689905,
      "Application logical size": {
        "value": 57936620039369
      },
      "Number of files": {
        "value": 99689905
      },
      "Average of size (app logical)": {
        "value": 581168.3744644857
      },
      "Physical data size": {
        "value": 83851860164608
      }
    },
    "f200_7.5tb-ssd_96gb": {
      "doc_count": 2077,
      "Application logical size": {
        "value": 185483427943
      },
      "Number of files": {
        "value": 2077
      },
      "Average of size (app logical)": {
        "value": 89303528.13818006
      },
      "Physical data size": {
        "value": 259958890496
      }
    },
    "a300_60tb_3.2tb-ssd_96gb": {
      "doc_count": 48685909,
      "Application logical size": {
        "value": 67341817001781
      },
      "Number of files": {
        "value": 48685909
      },
      "Average of size (app logical)": {
        "value": 1383189.0660967426
      },
      "Physical data size": {
        "value": 39103887130624
      }
    }
  }
}
```










