from elasticsearch import Elasticsearch
import json
HOST = '10.3.70.221'


def conect():
    elastic_client = None
    elastic_client = Elasticsearch(
        hosts=[{'host': '10.3.70.221', 'port': 9200}])
    if elastic_client.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    search_object = {
        "size": 5000,
        "query": {
            "bool": {
                "must": [
                    {
                        "multi_match": {
                            "query": "điện tử",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "50%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "đèn, đèn chiếu sáng",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "50%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "tivi, tủ lạnh, máy giặt",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "25%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "phòng ngủ, phòng khách, nhà bếp, phòng bếp",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "25%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "điều hoà, quạt, lò vi sóng",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "20%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "samsung, sony, lg, toshiba, Sunhouse, Kangaroo, Bluestone, Asanzo",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "20%"
                        }
                    },
                    {
                        "multi_match": {
                            "query": "gia đình, sinh hoạt, tiêu dùng",
                            "fields": ["_all"],
                            "type": "phrase",
                            "minimum_should_match": "25%"
                        }
                    },

                ]
            }
        }
    }
    search_object = json.dumps(search_object)
    #res = elastic_client.search(index=-7823841914345959386)
    f = open('data_for_none_class.txt', 'w+')
    for i in range(1, 13, 1):
        if(i < 10):
            index_value = 'urldata_2020_0' + str(i)
        else:
            index_value = 'urldata_2020_' + str(i)
        res = elastic_client.search(index=index_value, body=search_object)
        res = res['hits']['hits']
        print("thang " + str(i) + ":")
        print(len(res))
        for item in res:
            f.write("%s\n" % item)
        # for item in res['hits']['hits']:
            #print( item['_source']['content'] )

    # for item in res['hits']['hits']:
        #print( item['_source']['content'] )
    f.close()
    return elastic_client


conect()
