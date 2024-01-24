# Overview of access zone
GET _search
{
  "size": 0,
  "aggs": {
    "cdh633": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/cdh633"
              }
            },
            {
              "match": {
                "file_path": "/ifs/cdh633/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "dds-test-az": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/csc/dds_test_az"
              }
            },
            {
              "match": {
                "file_path": "/ifs/csc/dds_test_az/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "delldatascience": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/csc/dell_datascience"
              }
            },
            {
              "match": {
                "file_path": "/ifs/csc/dell_datascience/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "eyeglass": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/csc/igls/analyticsdb"
              }
            },
            {
              "match": {
                "file_path": "/ifs/csc/igls/analyticsdb/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "EyeglassRunbookRobot": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/poc/EyeglassRunbookRobot"
              }
            },
            {
              "match": {
                "file_path": "/ifs/poc/EyeglassRunbookRobot/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "h500f200-vdbench": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/csc/vdbench"
              }
            },
            {
              "match": {
                "file_path": "/ifs/csc/vdbench/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "noAD": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/demo/noAD"
              }
            },
            {
              "match": {
                "file_path": "/ifs/demo/noAD/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "vmstore": {
      "filter": {
        "bool": {
          "should": [
            {
              "match": {
                "file_path": "/ifs/csc/vmstore"
              }
            },
            {
              "match": {
                "file_path": "/ifs/csc/vmstore/*"
              }
            }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    },
    "System (excluding all other access zones)": {
      "filter": {
        "bool": {
          "must_not": [
            { "match": { "file_path": "/ifs/cdh633" } },
            { "match": { "file_path": "/ifs/cdh633/*" } },
            { "match": { "file_path": "/ifs/csc/dds_test_az" } },
            { "match": { "file_path": "/ifs/csc/dds_test_az/*" } },
            { "match": { "file_path": "/ifs/csc/dell_datascience" } },
            { "match": { "file_path": "/ifs/csc/dell_datascience/*" } },
            { "match": { "file_path": "/ifs/csc/igls/analyticsdb" } },
            { "match": { "file_path": "/ifs/csc/igls/analyticsdb/*" } },
            { "match": { "file_path": "/ifs/csc/vdbench" } },
            { "match": { "file_path": "/ifs/csc/vdbench/*" } },
            { "match": { "file_path": "/ifs/csc/vmstore" } },
            { "match": { "file_path": "/ifs/csc/vmstore/*" } },
            { "match": { "file_path": "/ifs/demo/noAD" } },
            { "match": { "file_path": "/ifs/demo/noAD/*" } },
            { "match": { "file_path": "/ifs/poc/EyeglassRunbookRobot" } },
            { "match": { "file_path": "/ifs/poc/EyeglassRunbookRobot/*" } }
          ]
        }
      },
      "aggs": {
        "Capacity": {
          "sum": {
            "field": "size"
          }
        }
      }
    }
  }
}


{
  "took": 4,
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
    "vmstore": {
      "doc_count": 278,
      "Capacity": {
        "value": 386954328601
      }
    },
    "dds-test-az": {
      "doc_count": 4,
      "Capacity": {
        "value": 60
      }
    },
    "noAD": {
      "doc_count": 2,
      "Capacity": {
        "value": 0
      }
    },
    "System (excluding all other access zones)": {
      "doc_count": 148406563,
      "Capacity": {
        "value": 125412027244988
      }
    },
    "EyeglassRunbookRobot": {
      "doc_count": 10,
      "Capacity": {
        "value": 1896
      }
    },
    "delldatascience": {
      "doc_count": 23,
      "Capacity": {
        "value": 47309
      }
    },
    "eyeglass": {
      "doc_count": 6,
      "Capacity": {
        "value": 1216
      }
    },
    "h500f200-vdbench": {
      "doc_count": 25,
      "Capacity": {
        "value": 17035
      }
    },
    "cdh633": {
      "doc_count": 54,
      "Capacity": {
        "value": 8659
      }
    }
  }
}
