import shutil

import pika
import pika.credentials
import os
import tqdm
import json

class RBQ_Client:

    def __init__(self, queue_name) -> None:
        self.queue_name = queue_name
        credentials = pika.PlainCredentials(username=os.getenv("RBQ_USER"),
                                            password=os.getenv("RBQ_PASS"))
        self.rbq = pika.ConnectionParameters(host="61.147.247.138", port=32010, virtual_host="/", credentials=credentials,
                                             heartbeat=0
                                             )
        self.connection = pika.BlockingConnection(self.rbq)
        self.channel = self.connection.channel()
        self.channel.basic_qos(prefetch_count=80)
        self.channel.queue_declare(queue=self.queue_name,durable=True)

    def publish_message(self, message):
        self.channel.basic_publish(exchange='', routing_key=self.queue_name, body=message)
        print("Message published successfully",message)

    def consume(self, callback):
        self.channel.basic_consume(queue=self.queue_name, on_message_callback=callback, auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        self.channel.start_consuming()

    def complete(self,method):
        self.channel.basic_ack(delivery_tag=method.delivery_tag)
        print(method.delivery_tag,' [*] messages completed')

    def get_message(self):
        method_frame, header_frame, body = self.channel.basic_get(queue=self.queue_name, auto_ack=False)
        if method_frame:
            return method_frame,body
        else:
            return None,None

    def close_connection(self):
        self.connection.close()


# 使用示例
if __name__ == '__main__':
    rbq_client = RBQ_Client("39net_wander")
    # for x in range(100,533):
    #     rbq_client.publish_message(str(x))
    # path=fr"zhuanlan_info.txt"
    # data=['http://m.39.net/woman/nxzx/', 'http://m.39.net/woman/nxbj/', 'http://m.39.net/fk/', 'http://m.39.net/woman/nxyy/', 'http://m.39.net/woman/nxqg/', 'http://m.39.net/man/jrtj/', 'http://m.39.net/man/nxbj/', 'http://m.39.net/nk/', 'http://m.39.net/man/nxsh/', 'http://m.39.net/baby/', 'http://m.39.net/baby/mmjk/', 'http://m.39.net/baby/bbjk/', 'http://m.39.net/fitness/baby/yeyp/ypxg/', 'http://m.39.net/oldman/lrqw/', 'http://m.39.net/oldman/lrbj/', 'http://m.39.net/oldman/lryp/', 'http://m.39.net/oldman/lrxl/', 'http://m.39.net/food/nutrition/', 'http://m.39.net/food/nutrition/', 'http://m.39.net/food/slys/', 'http://m.39.net/food/pr/', 'http://m.39.net/fitness/jrtj/', 'http://m.39.net/fitness/jfff/', 'http://m.39.net/fitness/jbjf/', 'http://m.39.net/fitness/jfzsk/ssjf/', 'http://m.39.net/sports/cs/', 'http://m.39.net/sports/skjs/', 'http://m.39.net/sports/cs/', 'http://m.39.net/sports/ydxm/', 'http://m.39.net/face/mrhf/hfcs/', 'http://m.39.net/face/mrhf/', 'http://m.39.net/face/mrcz/', 'http://m.39.net/face/mrxf/', 'http://m.39.net/care/', 'http://m.39.net/care/ys/', 'http://m.39.net/care/jbyf/', 'http://m.39.net/care/dzbj/', 'http://m.39.net/tj/', 'http://m.39.net/tj/women/', 'http://m.39.net/tj/man/', 'http://m.39.net/tj/grtj/', 'http://m.39.net/zl/yykx/', 'http://m.39.net/zl/zjft/', 'http://m.39.net/zl/yykx/', 'http://m.39.net/zl/hzgs/', 'http://m.39.net/cm/zyys/', 'http://m.39.net/cm/zyys/', 'http://m.39.net/cm/zyjb/', 'http://m.39.net/cm/tslf/', 'http://m.39.net/drug/yjxw/', 'http://m.39.net/drug/yjxw/', 'http://m.39.net/drug/jbyy/', 'http://m.39.net/drug/ypcssc/', 'http://m.39.net/xl/xltm/xlcs/', 'http://m.39.net/xl/xljk/', 'http://m.39.net/xl/hlxl/', 'http://m.39.net/xl/xlfw/', 'http://m.39.net/cancer/', 'http://m.39.net/nk/', 'http://m.39.net/fk/', 'http://m.39.net/gan/mxgy/', 'http://m.39.net/gan/gbzh/', 'http://m.39.net/hbv/', 'http://m.39.net/zl/hzgs/http://m.39.net/gan/treatment/', 'http://m.39.net/xh/bm/', 'http://m.39.net/xh/xhkzh/', 'http://m.39.net/wei/', 'http://m.39.net/xh/xhkzh/xhkxz/', 'http://m.39.net/cr/qt/', 'http://m.39.net/shen/', 'http://m.39.net/mn/yz/', 'http://m.39.net/mn/bj/', 'http://m.39.net/mouth/kqjb/', 'http://m.39.net/mouth/kqjb/', 'http://m.39.net/mouth/kqhl/', 'http://m.39.net/mouth/kqzj/', 'http://m.39.net/ebh/', 'http://m.39.net/zl/zjft/', 'http://m.39.net/ebh/bbjb/', 'http://m.39.net/ebh/yhjb/', 'http://m.39.net/eye/ykcs/', 'http://m.39.net/eye/ybfy/', 'http://m.39.net/eye/ybzh//', 'http://m.39.net/eye/zl/', 'http://m.39.net/zl/yykx/', 'http://m.39.net/hx/shxd/', 'http://m.39.net/hx/xhxd/', 'http://m.39.net/hx/gd/', 'http://m.39.net/news/jrtj/', 'http://m.39.net/news/kyfx/', 'http://m.39.net/news/medicine/', 'http://m.39.net/nk/szgr/', 'http://m.39.net/nk/szgr/', 'http://m.39.net/nk/xz/', 'http://m.39.net/nk/xza/', 'http://m.39.net/fk/', 'http://m.39.net/fk/szxtsl/', 'http://m.39.net/fk/', 'http://m.39.net/fk/yqxts/', 'http://m.39.net/ck/yqzn/', 'http://m.39.net/ck/yqzb/', 'http://m.39.net/ck/yqzn/', 'http://m.39.net/ck/chzn/', 'http://m.39.net/byby/zhlm/', 'http://m.39.net/byby/nxby/', 'http://m.39.net/byby/nanxby/', 'http://m.39.net/byby/yycs/', 'http://m.39.net/cancer/', 'http://m.39.net/cancer/azzl/', 'http://m.39.net/cancer/care/', 'http://m.39.net/cancer/cs/', 'http://m.39.net/cancer/', 'http://m.39.net/cancer/', 'http://m.39.net/heart/', 'http://m.39.net/hx/', 'http://m.39.net/zl/yykx/', 'http://m.39.net/hx/shxd/', 'http://m.39.net/hx/xhxd/', 'http://m.39.net/hx/gd/', 'http://m.39.net/heart/', 'http://m.39.net/heart/cs/', 'http://m.39.net/heart/', 'http://m.39.net/heart/care/', 'http://wei.39.net/cs/', 'http://m.39.net/wei/cs/', 'http://m.39.net/wei/bwyw/', 'http://m.39.net/wei/qtwbzl/', 'http://m.39.net/shen/cs/', 'ttp://m.39.net/shen/cs/', 'http://m.39.net/shen/zhlm/', 'http://m.39.net/shen/treatment/', 'http://m.39.net/tnb/', 'http://m.39.net/tnb/cs/', 'http://m.39.net/tnb/treatment/', 'http://m.39.net/tnb/care/', 'http://m.39.net/gan/mxgy/', 'http://m.39.net/gan/gbzh/', 'http://m.39.net/hbv/', 'http://m.39.net/zl/hzgs/http://m.39.net/gan/treatment/', 'http://m.39.net/zl/yykx/', 'http://m.39.net/hbv/cs/', 'http://m.39.net/hbv/treatment/', 'http://m.39.net/hbv/care/', 'http://m.39.net/aids/yf/', 'http://m.39.net/aids/yf/cs/', 'http://m.39.net/aids/yf/cb/', 'http://m.39.net/aids/jczl/', 'http://m.39.net/sj/cjjb/', 'http://m.39.net/sj/cjjb/', 'http://m.39.net/sj/zhlm/sjkbk/', 'http://m.39.net/sj/zhlm/zl/', 'http://m.39.net/xy/', 'http://m.39.net/xy/', 'http://m.39.net/xy/xybzl/', 'http://m.39.net/xy/xybkf/', 'http://m.39.net/js/', 'http://m.39.net/js/xljb/', 'http://m.39.net/js/xljb/qita/', 'http://m.39.net/js/jsbzx/', 'http://m.39.net/xh/bm/', 'http://m.39.net/xh/xhkzh/', 'http://m.39.net/wei/', 'http://m.39.net/xh/xhkzh/xhkxz/', 'http://m.39.net/cr/qt/', 'http://m.39.net/gm/xzh/', 'http://m.39.net/cr/zl/', 'http://m.39.net/cr/zl/lg/', 'http://m.39.net/shen/cs/', 'http://m.39.net/pf/qt/', 'http://m.39.net/pf/yf/', 'http://m.39.net/pf/zl/', 'http://m.39.net/xb/', 'http://m.39.net/xb/qt/qt/', 'http://m.39.net/xb/zl/', 'http://m.39.net/xb/yf/', 'http://m.39.net/fs/fskzx/', 'http://m.39.net/fs/fskzx/', 'http://m.39.net/fs/tf/', 'http://m.39.net/fs/qtfsb/', 'http://m.39.net/nfm/nfmzh/', 'http://m.39.net/tnb/', 'http://m.39.net/nfm/nfmjb/jzxjb/', 'http://m.39.net/nfm/xqdxjb/', 'http://m.39.net/wk/', 'http://m.39.net/wk/wc/', 'http://m.39.net/wk/nwk/', 'http://m.39.net/wk/gd/', 'http://m.39.net/gk/', 'http://m.39.net/gk/guzhe/', 'http://m.39.net/gk/jyzjb/', 'http://m.39.net/gk/qtgk/', 'http://m.39.net/gc/zcjb/', 'http://m.39.net/gc/gmjk/', 'http://m.39.net/gc/gcjbyfbj/', 'http://m.39.net/gc/zl/', 'http://m.39.net/cr/qt/', 'http://m.39.net/shen/', 'http://m.39.net/mn/yz/', 'http://m.39.net/mn/bj/', 'http://m.39.net/ebh/', 'http://m.39.net/ebh/ebjb/', 'http://m.39.net/ebh/bbjb/', 'http://m.39.net/ebh/yhjb/', 'http://m.39.net/eye/ykcs/', 'http://m.39.net/eye/ybfy/', 'http://m.39.net/eye/ybzh//', 'http://m.39.net/eye/zl/', 'http://m.39.net/mouth/kqjb/', 'http://m.39.net/mouth/kqjb/', 'http://m.39.net/mouth/kqhl/', 'http://m.39.net/mouth/kqzj/', 'http://m.39.net/ebh/', 'http://m.39.net/zl/zjft/', 'http://m.39.net/ebh/bbjb/', 'http://m.39.net/ebh/yhjb/', 'http://m.39.net/gm/xzh/', 'http://m.39.net/gm/qtgmxjb/', 'http://m.39.net/gm/zh/yf/', 'http://m.39.net/gm/pfgm/', 'http://m.39.net/ek/zl/', 'http://m.39.net/ek/yfjz/', 'http://m.39.net/ek/fs/', 'http://m.39.net/ek/ekzxky/', 'http://m.39.net/cm/zyys/', 'http://m.39.net/cm/zyys/', 'http://m.39.net/cm/zyjb/', 'http://m.39.net/cm/tslf/', 'http://m.39.net/tj/', 'http://m.39.net/tj/women/', 'http://m.39.net/tj/man/', 'http://m.39.net/tj/grtj/', 'http://m.39.net/xl/xltm/xlcs/', 'http://m.39.net/xl/xljk/', 'http://m.39.net/xl/hlxl/', 'http://m.39.net/xl/xlfw/', 'http://m.39.net/120/', 'http://m.39.net/120/jbjj/', 'http://m.39.net/120/ydjj/', 'http://m.39.net/120/sh/', 'http://m.39.net/cancer/', 'http://m.39.net/nk/', 'http://m.39.net/fk/']
    data=["https://www.39.net/"]
    for msg in tqdm.tqdm(data):
        rbq_client.publish_message(msg)
