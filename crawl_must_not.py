from elasticsearch import Elasticsearch
import json
# HOST = '10.3.70.221'


def conect():
    elastic_client = None
    elastic_client = Elasticsearch(
        hosts=[{'host': '10.3.70.221', 'port': 9200}])
    if elastic_client.ping():
        print('Yay Connect')
    else:
        print('Awww it could not connect!')
    search_object = {
        "size": 100,
        "query": {
            "bool": {
                "must_not": [
                    {
                        "multi_match": {
                            "query": "điện tử",
                            "fields": ["_all"],
                            "type": "phrase",
                           
                        }
                    },
                    {
                        "multi_match": {
                            "query": "đèn",
                            "fields": ["_all"],
                            "type": "phrase",
                         
                        }
                    },
                    # {
                    #     "multi_match": {
                    #         "query": "tivi",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                       
                    #     }
                    # },
                    # {
                    #     "multi_match": {
                    #         "query": "tủ lạnh",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                       
                    #     }
                    # },
                    # {
                    #     "multi_match": {
                    #         "query": "máy giặt",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                       
                    #     }
                    # },
                    # {
                    #     "multi_match": {
                    #         "query": "phòng ngủ",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                 
                    #     }
                    # },
                    # {
                    #     "multi_match": {
                    #         "query": "phòng bếp",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                 
                    #     }
                    # },
                    # {
                    #     "multi_match": {
                    #         "query": "nhà bếp",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                 
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "điều hoà",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "quạt",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "lò vi sóng",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "samsung",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "sony",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "lg",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "toshiba",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "Sunhouse",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "Kangaroo",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "Bluestone",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },

                    # {
                    #     "multi_match": {
                    #         "query": "Asanzo",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                
                    #     }
                    # },






                    # {
                    #     "multi_match": {
                    #         "query": "gia đình",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },


                    # {
                    #     "multi_match": {
                    #         "query": "sinh hoạt",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },


                    # {
                    #     "multi_match": {
                    #         "query": "tiêu dùng",
                    #         "fields": ["_all"],
                    #         "type": "phrase",
                      
                    #     }
                    # },

                ]
            }
        }
    }
    search_object = json.dumps(search_object)
    #res = elastic_client.search(index=-7823841914345959386)
    f = open('data_must_not.txt', 'w+')
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
