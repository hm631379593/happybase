# coding=utf-8

import pandas as pd
import requests
import re
def Readcsv(pathsw):
    data = {}
    try:
        brics = pd.read_csv(pathsw, encoding='gbk')
    except:
        brics = pd.read_csv(pathsw, encoding='utf-8')
    for colnmusw in brics.columns:
        # print(brics[colnmusw])
        listsw = []
        for c in brics.index:
            listsw.append(brics.loc[c, colnmusw])
        data_str = str(listsw)[1:len(str(listsw)) - 1]
        data_str = re.sub('(\\\\r)|(\\\\n)', '', data_str)
        data[colnmusw] = data_str
    return {'招标': data}
def send_request(realpathsw):
    url = 'http://192.168.1.50:9000/hdfs/add'
    payload = {'name': 'save', 'hdfs_path': '/', 'options': Readcsv(realpathsw)}
    r = requests.post(url=url, json=payload)
    print(r)
if __name__ == '__main__':
    paths = "F:\项目\大数据\路面机械数据\搬运车.csv"
    f = Readcsv(paths)
    print(f)
    #send_request(paths)