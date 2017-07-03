curl -XDELETE http://192.168.1.152:9200/rec-label
curl -XPUT 'http://192.168.1.152:9200/rec-label?pretty' -H 'Content-Type: application/json'  -d '{
    "settings":{
         "index" : {
            "number_of_shards":5,
            "number_of_replicas":1
         }
     },
      "mappings" : {
            "_default_" : {
                  "properties" : {
                    "lastUpdateTime": {
                        "type":"date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                    },
                    "labelCreateTime": {
                        "type":"date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                    },
                    "birthday": {
                        "type": "keyword"
                    },
                    "checkDate" : {
                        "type":"date",
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||epoch_millis"
                    },
                    "height": {
                        "type": "keyword"
                    },
                    "weight": {
                        "type": "keyword"
                    },
                    "waistline": {
                       "type": "keyword"
                    },
                    "sex": {
                       "type": "keyword"
                    },
                    "label": {
                         "type": "text"
                    },
                    "idCardNoMd5":{
                        "type": "keyword"
                    },
                    "checkUnitCode":{
                        "type": "keyword"
                    },
                    "userId":{
                        "type": "keyword"
                    },
                    "checkUnitName":{
                        "type": "keyword"
                    }
                 }
           }
      }
}';

