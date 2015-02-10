# -*- coding: utf-8 -*-
"""
This code enables to:
    - connect to the Cassandra cluster on a specified keyspace
    - send queries to the Cassandra table containing the preprocessed data (with spark)
    - the queries are built with Python:
        1. select cities being inside a 500km radius circle whose center is the seism epicenter
        2. select telephone numbers, latitude, longitude corresponding to these cities, in a time range from t0 = date_seism to t0 + 1hour
Create:
    - CREATE KEYSPACE test WITH replication = {   'class': 'SimpleStrategy',   'replication_factor': 2 };
    - create table cassandraresult (tel text,lat text,longi text, PRIMARY KEY (tel, lat));
    - create table test_spark_bigText(t timestamp, id_ville text, tels text, primary key ((t,id_ville)));
"""

from cassandra.cluster import Cluster

# Connect to the cluster
cluster = Cluster()
session = cluster.connect()

# PARAMETRES
session.execute("USE test;")

#-------------------------------------------------------------------------------------------------#
# select cities, send queries to Cassandra
#-------------------------------------------------------------------------------------------------#
from selection_villes import findListVilles, getClosest
import datetime
import time
import math
from cassandra.query import BatchStatement
from cassandra.query import SimpleStatement
import os

# round hour e.g. 23:44 -> 23:40
def round_up(tm):
    upmins = math.ceil(float(tm.minute)/10-1)*10
    diffmins = upmins - tm.minute
    newtime = tm + datetime.timedelta(minutes=diffmins)
    newtime = newtime.replace(second=0)
    return newtime

# function that insert the results of a the queries to a Cassandra table "cassandraresult"
def insertbatch(rowsToAdd,session):
    batch = BatchStatement()
    for row in rowsToAdd:
        batch.add(SimpleStatement("INSERT INTO cassandraresult(tel,lat,longi) values(%s,%s,%s)"),(row[2],row[0],row[1]))
    session.execute(batch)

# select Tel, lat and long being in the cities in the seism area: perform queries
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

    # request on CASSANDRA, batch size = 10000
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
                print "insert batch " + str(i)

    return Result


#------------------------------------------------------------------------------------------------#
# Requête

# seism info:
Lat_seism  = 35.01
Long_seism = 135.0
time_seism = '2015-01-25 10:50'

#IPaddresses of the 5 clusters
IPaddressesTables=['172.31.53.39','172.31.53.38','172.31.53.40','172.31.53.41', '172.31.53.41']

# Shut the closest node down
_,nodeToCut=getClosest(Lat_seism, Long_seism)
os.system("nodetool -h "+IPaddressesTables[nodeToCut]+" stopdaemon")


# parametre
session.execute("USE test;")

# run functions
Result = Requetage(Lat_seism, Long_seism, time_seism)


