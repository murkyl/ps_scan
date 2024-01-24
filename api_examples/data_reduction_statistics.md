# Data reduction statistics
GET _search
{
  "size": 0,
  "aggs": {
    "Deduped files": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "file_is_deduped": true
              }
            }
          ]
        }
      },
      "aggs": {
        "Files": {
          "value_count": {
            "field": "file_is_deduped"
          }
        },
        "Physical size": {
          "sum": {
            "field": "size_physical_data"
          }
        },
        "Logical size": {
          "sum": {
            "field": "size_logical"
          }
        }
      }
    },
    "Files inlined with inode": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "file_is_inlined": true
              }
            }
          ]
        }
      },
      "aggs": {
        "Files": {
          "value_count": {
            "field": "file_is_inlined"
          }
        },
        "Physical size": {
          "sum": {
            "field": "size_physical_data"
          }
        },
        "Logical size": {
          "sum": {
            "field": "size_logical"
          }
        }
      }
    },
    "Compressed files": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "file_is_compressed": true
              }
            }
          ]
        }
      },
      "aggs": {
        "Files": {
          "value_count": {
            "field": "file_is_compressed"
          }
        },
        "Physical size": {
          "sum": {
            "field": "size_physical_data"
          }
        },
        "Logical size": {
          "sum": {
            "field": "size_logical"
          }
        }
      }
    },
    "Compressed and inlined": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "file_is_compressed": true
              }
            },
            {
              "match": {
                "file_is_compressed": true
              }
            }
          ]
        }
      },
      "aggs": {
        "Files": {
          "value_count": {
            "field": "file_is_compressed"
          }
        },
        "Physical size": {
          "sum": {
            "field": "size_physical_data"
          }
        },
        "Logical size": {
          "sum": {
            "field": "size_logical"
          }
        }
      }
    },
    "Packed files": {
      "filter": {
        "bool": {
          "must": [
            {
              "match": {
                "file_is_packed": true
              }
            }
          ]
        }
      },
      "aggs": {
        "Files": {
          "value_count": {
            "field": "file_is_packed"
          }
        },
        "Physical size": {
          "sum": {
            "field": "size_physical_data"
          }
        },
        "Logical size": {
          "sum": {
            "field": "size_logical"
          }
        }
      }
    }  
  }
}

{
  "took": 3974,
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
    "Packed files": {
      "doc_count": 0,
      "Physical size": {
        "value": 0
      },
      "Logical size": {
        "value": 0
      },
      "Files": {
        "value": 0
      }
    },
    "Files inlined with inode": {
      "doc_count": 145815,
      "Physical size": {
        "value": 0
      },
      "Logical size": {
        "value": 1117159424
      },
      "Files": {
        "value": 145815
      }
    },
    "Compressed and inlined": {
      "doc_count": 33323291,
      "Physical size": {
        "value": 10180697595904
      },
      "Logical size": {
        "value": 31260403941376
      },
      "Files": {
        "value": 33323291
      }
    },
    "Compressed files": {
      "doc_count": 33323291,
      "Physical size": {
        "value": 10180697595904
      },
      "Logical size": {
        "value": 31260403941376
      },
      "Files": {
        "value": 33323291
      }
    },
    "Deduped files": {
      "doc_count": 654,
      "Physical size": {
        "value": 99345743872
      },
      "Logical size": {
        "value": 372040278016
      },
      "Files": {
        "value": 654
      }
    }
  }
}
