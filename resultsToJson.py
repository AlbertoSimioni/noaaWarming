#to run the script type on the shell: python resultsToJson.py
import csv
import json
import sys
import os

from subprocess import call
startYear = 1901
numYearsGrouped = 5
for i in range(0,23): #0,23
    currYearsString = str(startYear+i*numYearsGrouped) +"_"+str(startYear+4+i*numYearsGrouped)
    call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+currYearsString+ "/avgs" ,"/home/lsde06/avgs/"+ currYearsString+".csv"])
    call(["hdfs","dfs", "-getmerge","/user/lsde06/results"+currYearsString + "/minmax" ,"/home/lsde06/minmax/"+ currYearsString+".csv"])


    fieldnames=["lat","long","avgTemp","stddev"]
    f=open("avgs/"+currYearsString+'.csv', 'r')
    csv_reader = csv.DictReader(f,fieldnames)
    jsonf = open("avgs/"+currYearsString+'.json', 'w')
    data = json.dumps({'results':[r for r in csv_reader]})
    jsonf.write(data)
    f.close()
    jsonf.close()
    #os.remove("avgs/"+currYearsString+'.csv')


    fieldnames=["lat","long","minTemp","maxTemp"]
    f=open("minmax/"+currYearsString+'.csv', 'r')
    csv_reader = csv.DictReader(f,fieldnames)
    jsonf = open("minmax/"+currYearsString+'.json', 'w')
    data = json.dumps({'results':[r for r in csv_reader]})
    jsonf.write(data)
    f.close()
    jsonf.close()
    #os.remove("minmax/"+currYearsString+'.csv')