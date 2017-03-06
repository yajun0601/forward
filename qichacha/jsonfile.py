# -*- coding: utf-8 -*-
import json
import time



def store(data):
    with open('data.json', 'w') as json_file:
        json_file.write(json.dumps(data))

def load():
    with open('JudgementDetail.json') as json_file:
        data = json.load(json_file)
        return data



def loadFont():
    f = open("SearchInvestment.json", encoding='utf-8')  #设置以utf-8解码模式读取文件，encoding参数必须设置，否则默认以gbk模式读取文件，当文件中包含中文时，会报错
    setting = json.load(f)
#    family = setting['BaseSettings']['size']   #注意多重结构的读取语法
#    size = setting['fontSize']   
    return setting




if __name__ == "__main__":
    data = load()
    print(data)
    t = loadFont()

    print(t)
    data = {}
    data["last"]=time.strftime("%Y%m%d")
#    store(data)

    data = load()
    print(data)