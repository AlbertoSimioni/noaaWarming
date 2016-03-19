#to run the script type on the shell: python resultsToJson.py year
import csv
import json
import sys
import os

from subprocess import call

call(["hdfs","dfs", "-getmerge","/user/lsde06/1980stations" ,"/home/lsde06/stations/1980"+".csv"])

#csvfile = open("results"+str(sys.argv[1])+'.csv', 'r')
#jsonfile = open("results"+str(sys.argv[1])+'.json', 'w')



fieldnames=["lat","long"]
f=open("stations/1980"+'.csv', 'r')
csv_reader = csv.DictReader(f,fieldnames)
jsonf = open("stations/1980"+'.json', 'w')
data = json.dumps({'positions':[r for r in csv_reader]})
jsonf.write(data)
f.close()
jsonf.close()
# os.remove("avgs/"+str(year)+'.csv')
