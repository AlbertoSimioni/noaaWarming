#to run the script type on the shell: python resultsToJson.py year
import csv
import json
import sys

from subprocess import call
call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+sys.argv[1] + "/" ,"/home/lsde06/results"+ sys.argv[1]+".csv"])

#csvfile = open("results"+str(sys.argv[1])+'.csv', 'r')
#jsonfile = open("results"+str(sys.argv[1])+'.json', 'w')



fieldnames=["stationID","avgTemp","latitude","longitude"]
f=open("results"+str(sys.argv[1])+'.csv', 'r')
csv_reader = csv.DictReader(f,fieldnames)
jsonf = open("results"+str(sys.argv[1])+'.json', 'w')
data = json.dumps({'results':[r for r in csv_reader]})
jsonf.write(data)
f.close()
jsonf.close()


