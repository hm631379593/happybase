import json
import  pandas as pd
import csv
json1 = '{"a":1,"b":2,"c":3,"d":4,"e":5}';
url="F:\项目\大数据\路面机械数据\搬运车.csv"
f=open(url,encoding="gbk")
for i in range(2):
    f.readline()
k=csv.DictReader(f)
d=""
for i in k:
    d+=json.dumps(i)
print(d)