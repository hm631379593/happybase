import  csv
import sys
filename="F:\项目\大数据\路面机械数据\搬运车.csv"
data=[]
try:
    with open(filename) as f:
       header=f.readline()
       data=[row for row in f]
except csv.Error as e:
    print("Error reading SCV fie at line %s:%s"%(len(f),e))
    sys.exit(-1)

if header:
    print(header)
    print("====================")
for datarow in data:
    print(datarow)