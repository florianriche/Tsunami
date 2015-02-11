# Tsunami

Check our [presentation](http://slides.com/florianriche/arato-tsunami/fullscreen)

#USER GUIDE

##1-AWS cluster initialization
Follow documentation from [Using OpsCenter to create a cluster on Amazon EC2](
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIOpsc.html)

Create the key pair 
```javascript
chmod 400 <my-key-pair>.pem
```

##2-Launch the AMI (us-east-1	ami-f9a2b690)
Follow documentation [Installing on Amazon EC2> Launch the AMI](
http://www.datastax.com/documentation/datastax_enterprise/4.6/datastax_enterprise/install/installAMIlaunch.html)

In Advandced details, set parameters:

```javascript
--version enterprise
--analytics nodes 6
--totalnodes 6
--username datastaxusername
--password datastaxpassword
````

##3-Connect to Spark Master :
```javascript
ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "dse spark"
```
##4-Connect to Cassandra Master:

```javascript
ssh -i <my-key-pair>.pem ubuntu@<ip-master> then launch "cqlsh"
````

##5-On Spark, import data from S3, preprocess the data and save to Cassandra:

On spark terminal, copy/paste the file sparkCSV.scala

##6-Install Python Librairies on the AMIs:
Git clone this repository and then execute
```javascript
sh config_python.sh
```

##7- Create keyspaces and tables on Cassandra

```javascript
CREATE KEYSPACE test WITH replication = {   'class': 'SimpleStrategy',   'replication_factor': 2 };
create table cassandraresult (seismetime text, tel text,lat text,longi text, warnedtime text, PRIMARY KEY (seismetime,tel));
create table test_spark_bigText(t timestamp, id_ville text, tels text, primary key ((t,id_ville)));
````
##8-Set parameters in Requetage.py
Insert the IP adresses of the 5 workers nodes in the table IPaddressesTables (line 124)
```javascript
IPaddressesTables=['172.31.53.38','172.31.53.39','172.31.53.40','172.31.53.41', '172.31.53.41']
```
##9-Launch the python file requetage.py
Enter the longitude, latitude and the time values when asked
```javascript
latitude : 35.01
longitude : 135.0
datetime YYYY-MM-DD HH:MM: 2015-01-25 10:50
```
