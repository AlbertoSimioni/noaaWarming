#to run the script type on the shell: python resultsToJson.py year
import csv
import json
import sys
import os

from subprocess import call

years = [1907, 1930, 1945, 1962, 1968, 1972, 2014]
for year in years:
    call(["hdfs","dfs", "-getmerge","/user/lsde06/"+str(year)+"stations" ,"/home/lsde06/stations/"+str(year)+".csv"])

    #csvfile = open("results"+str(sys.argv[1])+'.csv', 'r')
    #jsonfile = open("results"+str(sys.argv[1])+'.json', 'w')



    fieldnames=["lat","long"]
    f=open("stations/"+str(year)+'.csv', 'r')
    csv_reader = csv.DictReader(f,fieldnames)
    jsonf = open("stations/"+str(year)+'.json', 'w')
    data = json.dumps({'positions':[r for r in csv_reader]})
    jsonf.write(data)
    f.close()
    jsonf.close()
    # os.remove("avgs/"+str(year)+'.csv')
