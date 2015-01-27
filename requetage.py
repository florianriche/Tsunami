# -*- coding: utf-8 -*-

from cassandra.cluster import Cluster

"""
1. lancer VM
2. se connecter en ssh
3. sur la VM: ccm start puis ccm status node1
   ccm node1 show
4.
!!!!!!!!!!! (cf sujet tp)
Pour acceder a ces ports depuis la machine hote on doit configurer une redirection des ports via
un tunnel SSH. Par exemple pour rediriger le port local 9042 vers le port 9042 du noeud 1 du
cluster (qui tourne sur l’ip 127.0.0.1) lancer la commande suivante:

ssh -nNT -L 9042:127.0.0.1:9042 bigdata@192.168.56.101
"""

# CONNECTION: After the driver connects to one of these nodes it will automatically discover the rest of the nodes in the cluster and connect to them, so you don’t need to list every node in your cluster.
cluster = Cluster(['127.0.0.1']) # 127.0.0.1 parce qu'on a créé un tunnel ssh
session = cluster.connect()

# DROP keyspace and table
#session.execute("DROP Keyspace test1")

# CREATE keyspace
session.execute("CREATE KEYSPACE test1 WITH replication = {'class': 'SimpleStrategy','replication_factor': 3 };")
session.execute("USE test1");

# CREATE TABLE
session.execute("CREATE TABLE Tsunami_test1 (T timestamp, Id_Ville text, tel int, lat float, long float, PRIMARY KEY ((T, Id_Ville), tel));")

# INSERT
session.execute("INSERT INTO Tsunami_test1 (T, Id_Ville, tel, lat, long) VALUES('2015-01-01 23:44', 'Tok', 34567, 35.00, 135.00);")


#-------------------------------------------------------------------------------------------------#
from selection_villes import findListVilles
import datetime

# select Tel, lat and long being in the cities in the seism area
def Requetage(SeismeLatitude,SeismeLongitude, timestampTdT):
    # select villes
    Villes=findListVilles(SeismeLatitude,SeismeLongitude)
    # convert string to datetime
    time = datetime.datetime.strptime(timestampTdT, '%Y-%m-%d %H:%M')
    Intervalles=[timestampTdT]
    Result = []
    # select an hour from timestampTdT
    for i in range(10,70,10):
        time = time+datetime.timedelta(0,0,0,0,i,0,0)
        # convert time to string
        strTime = time.strftime('%Y-%m-%d %H:%M')
        Intervalles.append(strTime)

    #print Intervalles
    #print Villes

    # request on CASSANDRA
    for ville in Villes:
        for t in Intervalles:
            Result.append(session.execute("SELECT Tel, lat, long FROM Tsunami_test1 WHERE T = %s AND Id_Ville = %s;", (t, ville)))

    return Result


#------------------------------------------------------------------------------------------------#
# Test de requête

#Requetage(35.01, 135.0, datetime.datetime(2015,01,01,23,44))
Result = Requetage(35.01, 135.0, '2015-01-01 23:44')
print Result
