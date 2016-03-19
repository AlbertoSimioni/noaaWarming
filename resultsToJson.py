#to run the script type on the shell: python resultsToJson.py
import csv
import json
import sys
import os

from subprocess import call

for year in range(1901, 1911):
    call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+str(year) + "/avgs" ,"/home/lsde06/avgs/"+ str(year)+".csv"])
    call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+str(year) + "/minmax" ,"/home/lsde06/minmax/"+ str(year)+".csv"])


    fieldnames=["lat","long","temp"]
    f=open("avgs/"+str(year)+'.csv', 'r')
    csv_reader = csv.DictReader(f,fieldnames)
    jsonf = open("avgs/"+str(year)+'.json', 'w')
    data = json.dumps({'results':[r for r in csv_reader]})
    jsonf.write(data)
    f.close()
    jsonf.close()
    os.remove("avgs/"+str(year)+'.csv')


    fieldnames=["lat","long","minTemp","maxTemp"]
    f=open("minmax/"+str(year)+'.csv', 'r')
    csv_reader = csv.DictReader(f,fieldnames)
    jsonf = open("minmax/"+str(year)+'.json', 'w')
    data = json.dumps({'results':[r for r in csv_reader]})
    jsonf.write(data)
    f.close()
    jsonf.close()
    os.remove("minmax/"+str(year)+'.csv')