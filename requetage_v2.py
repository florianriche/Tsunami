# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster

cluster = Cluster() # 127.0.0.1 parce qu'on a créé un tunnel ssh
session = cluster.connect()

# CREATE CASSANDRARESULT TABLE

# PARAMETRES
#keyspace = "test"
table    = "test_spark_bigtext"
session.execute("USE test;")

#-------------------------------------------------------------------------------------------------#
from selection_villes import findListVilles
import datetime
import time
import math
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement

# round hour e.g. 23:44 -> 23:40
def round_up(tm):
    upmins = math.ceil(float(tm.minute)/10-1)*10
    diffmins = upmins - tm.minute
    newtime = tm + datetime.timedelta(minutes=diffmins)
    newtime = newtime.replace(second=0)
    return newtime

def insertbatch(rowsToAdd,session):
    batch = BatchStatement()
    for row in rowsToAdd:
        batch.add(SimpleStatement("INSERT INTO cassandraresult(tel,lat,longi) values(%s,%s,%s)"),(row[0],row[1],row[2]))
    session.execute(batch)


# select Tel, lat and long being in the cities in the seism area
def Requetage(SeismeLatitude,SeismeLongitude, timestampTdT):
    # select villes
    Villes=findListVilles(SeismeLatitude,SeismeLongitude)
    # convert string to datetime
    time = round_up(datetime.datetime.strptime(timestampTdT, '%Y-%m-%d %H:%M'))
    Intervalles=[time.strftime('%Y-%m-%d %H:%M')]
    Result = []
    # select an hour from timestampTdT
    for i in range(10,70,10):
        time = time+datetime.timedelta(0,0,0,0,10,0,0)
        # convert time to string
        strTime = time.strftime('%Y-%m-%d %H:%M')
        Intervalles.append(strTime)
    # request on CASSANDRA

    start = time.time()
    for ville in Villes:
        for t in Intervalles:
            Result = session.execute("SELECT tels FROM test_spark_bigtext WHERE T = %s AND Id_Ville = %s;", (t, ville))
            print "ville : " , ville
            print "t : ", t
            if Result:
                rows = Result[0].tels.split("|")
                Batch = []
                batchSize=0
                i = 0
                for row in (rows[:-1]):
                    i+=1
                    batchSize=batchSize+1
                    Batch.append(row.split("/"))
                    if(batchSize==10000):
                         insertbatch(Batch,session)
                         Batch = []
                insertbatch(Batch,session)

                print "youpiiiiiii" + str(i)

    elapsed = (time.time() - start)
    print "elapsed time : " + str(elapsed)

    return Result


#------------------------------------------------------------------------------------------------#
# Test de requête

#Requetage(35.01, 135.0, datetime.datetime(2015,01,01,23,44))
Result = Requetage(35.01, 135.0, '2015-01-25 10:50')
#print Result
