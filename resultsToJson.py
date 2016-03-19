#to run the script type on the shell: python resultsToJson.py year
import csv
import json
import sys

from subprocess import call
call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+sys.argv[1] + "/avgs" ,"/home/lsde06/avgs/"+ sys.argv[1]+".csv"])
call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+sys.argv[1] + "/minmax" ,"/home/lsde06/minmax/"+ sys.argv[1]+".csv"])

#csvfile = open("results"+str(sys.argv[1])+'.csv', 'r')
#jsonfile = open("results"+str(sys.argv[1])+'.json', 'w')



fieldnames=["lat","long","temp"]
f=open("avgs/"+str(sys.argv[1])+'.csv', 'r')
csv_reader = csv.DictReader(f,fieldnames)
jsonf = open("avgs/"+str(sys.argv[1])+'.json', 'w')
data = json.dumps({'results':[r for r in csv_reader]})
jsonf.write(data)
f.close()
jsonf.close()


fieldnames=["lat","long","minTemp","maxTemp"]
f=open("minmax/"+str(sys.argv[1])+'.csv', 'r')
csv_reader = csv.DictReader(f,fieldnames)
jsonf = open("minmax/"+str(sys.argv[1])+'.json', 'w')
data = json.dumps({'results':[r for r in csv_reader]})
jsonf.write(data)
f.close()
jsonf.close()
