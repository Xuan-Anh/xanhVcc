from elasticsearch import Elasticsearch
import json
import csv
import pandas as pd
# HOST = '10.3.70.221'


class DataCollector:
    def __init__(self, HOST=None):
        self.HOST = HOST

    def collect(self, raw_output_file='raw_data.csv', key_output_file='key_data.csv'):
        elastic_client = None
        elastic_client = Elasticsearch(
            hosts=[{'host': '10.3.70.221', 'port': 9200}])
        if elastic_client.ping():
            print('Yay Connected')
        else:
            print('Awww it could not connect!')
        search_object = {
            "size": 10000,
            "query": {
                "bool": {
                    "should": [
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
                        {
                            "multi_match": {
                                "query": "tivi",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                        {
                            "multi_match": {
                                "query": "tủ lạnh",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                        {
                            "multi_match": {
                                "query": "máy giặt",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                        {
                            "multi_match": {
                                "query": "phòng ngủ",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                        {
                            "multi_match": {
                                "query": "phòng bếp",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                        {
                            "multi_match": {
                                "query": "nhà bếp",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "điều hoà",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "quạt",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "lò vi sóng",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "samsung",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "sony",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "lg",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "toshiba",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "Sunhouse",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "Kangaroo",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "Bluestone",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },

                        {
                            "multi_match": {
                                "query": "Asanzo",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },






                        {
                            "multi_match": {
                                "query": "gia đình",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },


                        {
                            "multi_match": {
                                "query": "sinh hoạt",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },


                        {
                            "multi_match": {
                                "query": "tiêu dùng",
                                "fields": ["_all"],
                                "type": "phrase",

                            }
                        },
                    ]
                }
            }
        }
        search_object = json.dumps(search_object)
        # res = elastic_client.search(index=-7823841914345959386)
        res = elastic_client.search(
            index='urldata_2020_02', body=search_object)
        data = res['hits']['hits']
        result = pd.DataFrame()
        headers = ['keywords']
        with open(key_output_file, 'w') as data_file:
            writer = csv.DictWriter(data_file, fieldnames=headers)
            writer.writeheader()
            for item in data:
                result = result.append(item['_source'], ignore_index=True)
                # print(item['_source'][])
                writer.writerow({'keywords': item['_source']['keywords']})

        result.to_csv(raw_output_file)


# data = [{
        # "_index": "urldata_2020_02",
        # "_type": "doc",
        # "_id": "-7705943844497689193",
        # "_score": 27.963434,
        # "_source": {
        #     "date": "2020-02-27 13:03:41",
        #     "timeCreate": 1582783421259,
        #     "priceStr": "",
        #     "keywords": "SUV 7 chỗ,Honda Thái Lan,thị trường Thái Lan,honda br-v,Honda,xe suv,xe thể thao đa dụng,xe ô tô Nhật,xe 7 chỗ,giá xe Honda,giá honda br-v,xe Honda",
        #     "domain": "autopro.com.vn",
        #     "tag": "SUV 7 chỗ, Honda Thái Lan, thị trường Thái Lan, honda br-v, Honda, xe suv, xe thể thao đa dụng, xe ô tô Nhật, xe 7 chỗ, giá xe Honda, giá honda br-v, xe Honda",
        #     "title": "SUV 7 chỗ Honda BR-V ra mắt tại Thái Lan với màu sơn mới",
        #     "category": "",
        #     "descriptions": "Ngoài màu sơn mới, Honda BR-V tại Thái Lan còn có một điểm nhấn khác biệt là động cơ có thể chạy bằng xăng hoặc nhiên liệu sinh học E85.",
        #     "url": "http://autopro.com.vn/quoc-te/suv-7-cho-honda-br-v-ra-mat-tai-thai-lan-voi-mau-son-moi-20151202111108622.chn",
        #     "content": "Ngoài màu sơn mới, Honda BR-V tại Thái Lan còn có một điểm nhấn khác biệt là động cơ có thể chạy bằng xăng hoặc nhiên liệu sinh học E85. Trong triển lãm xe quốc tế Thái Lan 2015, hãng Honda đã chính thức giới thiệu mẫu SUV 7 chỗ BR-V mới với người tiêu dùng tại đất nước chùa tháp. Theo ông Pitak Pruittisarikorn, giám đốc vận hành của Honda Thái Lan, BR-V sẽ có mặt trên thị trường này vào năm sau. Khi ra mắt trong triển lãm, Honda BR-V được sơn màu mới là cam. Trong khi đó, Honda BR-V tại thị trường Indonesia chỉ có 6 màu sơn ngoại thất là trắng, đỏ, đen, xám, xanh lục và bạc. Tại thị trường Thái Lan, Honda BR-V được trang bị động cơ 4 xi-lanh, i-VTEC, SOHC, dung tích 1,5 lít, sản sinh công suất tối đa 117 mã lực tại vòng tua máy 6.000 vòng/phút và mô-men xoắn cực đại 146 Nm tại vòng tua máy 4.700 vòng/phút. Đặc biệt, động cơ này có thể chạy bằng nhiên liệu sinh học E85. Sức mạnh được truyền tới bánh thông qua hộp số CVT. Trong khi đó, tại thị trường Indonesia, Honda BR-V sử dụng máy xăng i-VTEC, dung tích 1,5 lít, kết hợp với hộp số sàn 6 cấp hoặc CVT. Động cơ có thông số vận hành tương tự loại trên Honda BR-V tại Thái Lan nhưng chỉ chạy bằng xăng. Hãng Honda xác nhận BR-V tại thị trường Thái Lan sẽ được cung cấp những tính năng an toàn như chống bó cứng phanh ABS, 2 túi khí SRS, phân bổ lực phanh điện tử EBD, cân bằng điện tử và hỗ trợ khởi hành ngang dốc. Bên ngoài, Honda BR-V có đèn pha Projector, đèn định vị dạng LED và bộ la-zăng hợp kim 16 inch. Hiện chưa rõ giá bán cụ thể của Honda BR-V tại thị trường Thái Lan. Giá bán tương ứng của Honda BR-V tại thị trường Indonesia là 230 – 265 triệu Rupiah, tương đương 375 – 433 triệu Đồng. Theo kế hoạch, Honda BR-V sẽ bắt đầu được bày bán trên thị trường Indonesia trong tuần này. Hiện Honda Indonesia đã nhận khoảng 4.000 đơn đặt hàng dành cho BR-V mới. Tạm thời, chưa rõ Honda BR-V có được phân phối tại các thị trường Đông Nam Á khác như Việt Nam hay không.",
        #     "page_view": 1,
        #     "page_view_mobile": 1
        #     }
        # },
        # {
        # "_index": "urldata_2020_02",
        # "_type": "doc",
        # "_id": "-7705943844497689193",
        # "_score": 27.963434,
        # "_source": {
        #     "date": "2020-02-27 13:03:41",
        #     "timeCreate": 1582783421259,
        #     "priceStr": "",
        #     "keywords": "SUV 7 chỗ,Honda Thái Lan,thị trường Thái Lan,honda br-v,Honda,xe suv,xe thể thao đa dụng,xe ô tô Nhật,xe 7 chỗ,giá xe Honda,giá honda br-v,xe Honda",
        #     "domain": "autopro.com.vn",
        #     "tag": "SUV 7 chỗ, Honda Thái Lan, thị trường Thái Lan, honda br-v, Honda, xe suv, xe thể thao đa dụng, xe ô tô Nhật, xe 7 chỗ, giá xe Honda, giá honda br-v, xe Honda",
        #     "title": "SUV 7 chỗ Honda BR-V ra mắt tại Thái Lan với màu sơn mới",
        #     "category": "",
        #     "descriptions": "Ngoài màu sơn mới, Honda BR-V tại Thái Lan còn có một điểm nhấn khác biệt là động cơ có thể chạy bằng xăng hoặc nhiên liệu sinh học E85.",
        #     "url": "http://autopro.com.vn/quoc-te/suv-7-cho-honda-br-v-ra-mat-tai-thai-lan-voi-mau-son-moi-20151202111108622.chn",
        #     "content": "Ngoài màu sơn mới, Honda BR-V tại Thái Lan còn có một điểm nhấn khác biệt là động cơ có thể chạy bằng xăng hoặc nhiên liệu sinh học E85. Trong triển lãm xe quốc tế Thái Lan 2015, hãng Honda đã chính thức giới thiệu mẫu SUV 7 chỗ BR-V mới với người tiêu dùng tại đất nước chùa tháp. Theo ông Pitak Pruittisarikorn, giám đốc vận hành của Honda Thái Lan, BR-V sẽ có mặt trên thị trường này vào năm sau. Khi ra mắt trong triển lãm, Honda BR-V được sơn màu mới là cam. Trong khi đó, Honda BR-V tại thị trường Indonesia chỉ có 6 màu sơn ngoại thất là trắng, đỏ, đen, xám, xanh lục và bạc. Tại thị trường Thái Lan, Honda BR-V được trang bị động cơ 4 xi-lanh, i-VTEC, SOHC, dung tích 1,5 lít, sản sinh công suất tối đa 117 mã lực tại vòng tua máy 6.000 vòng/phút và mô-men xoắn cực đại 146 Nm tại vòng tua máy 4.700 vòng/phút. Đặc biệt, động cơ này có thể chạy bằng nhiên liệu sinh học E85. Sức mạnh được truyền tới bánh thông qua hộp số CVT. Trong khi đó, tại thị trường Indonesia, Honda BR-V sử dụng máy xăng i-VTEC, dung tích 1,5 lít, kết hợp với hộp số sàn 6 cấp hoặc CVT. Động cơ có thông số vận hành tương tự loại trên Honda BR-V tại Thái Lan nhưng chỉ chạy bằng xăng. Hãng Honda xác nhận BR-V tại thị trường Thái Lan sẽ được cung cấp những tính năng an toàn như chống bó cứng phanh ABS, 2 túi khí SRS, phân bổ lực phanh điện tử EBD, cân bằng điện tử và hỗ trợ khởi hành ngang dốc. Bên ngoài, Honda BR-V có đèn pha Projector, đèn định vị dạng LED và bộ la-zăng hợp kim 16 inch. Hiện chưa rõ giá bán cụ thể của Honda BR-V tại thị trường Thái Lan. Giá bán tương ứng của Honda BR-V tại thị trường Indonesia là 230 – 265 triệu Rupiah, tương đương 375 – 433 triệu Đồng. Theo kế hoạch, Honda BR-V sẽ bắt đầu được bày bán trên thị trường Indonesia trong tuần này. Hiện Honda Indonesia đã nhận khoảng 4.000 đơn đặt hàng dành cho BR-V mới. Tạm thời, chưa rõ Honda BR-V có được phân phối tại các thị trường Đông Nam Á khác như Việt Nam hay không.",
        #     "page_view": 1,
        #     "page_view_mobile": 1
        #     }
        # }
        # ]
